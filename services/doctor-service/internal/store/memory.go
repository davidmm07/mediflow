package store

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/davidmm07/mediflow/services/doctor-service/internal/domain"
)

// Memory is an in-memory Store used by unit tests and, more importantly, by
// Pact provider verification: each provider state seeds this store instead
// of a real MongoDB, so contract verification runs in CI with no containers.
type Memory struct {
	mu      sync.RWMutex
	doctors map[string]domain.Doctor
	slots   map[string]domain.Slot
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{
		doctors: make(map[string]domain.Doctor),
		slots:   make(map[string]domain.Slot),
	}
}

// Reset clears all state, called between provider states.
func (m *Memory) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doctors = make(map[string]domain.Doctor)
	m.slots = make(map[string]domain.Slot)
}

func (m *Memory) CreateDoctor(_ context.Context, d domain.Doctor) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, existing := range m.doctors {
		if existing.KeycloakUserID == d.KeycloakUserID {
			return domain.ErrDuplicate
		}
	}
	m.doctors[d.ID] = d
	return nil
}

func (m *Memory) GetDoctor(_ context.Context, id string) (domain.Doctor, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	d, ok := m.doctors[id]
	if !ok {
		return domain.Doctor{}, domain.ErrNotFound
	}
	return d, nil
}

func (m *Memory) ListDoctors(_ context.Context, specialty string) ([]domain.Doctor, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := []domain.Doctor{}
	for _, d := range m.doctors {
		if !d.Active {
			continue
		}
		if specialty != "" && d.Specialty != specialty {
			continue
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FullName < out[j].FullName })
	return out, nil
}

func (m *Memory) CreateSlot(_ context.Context, s domain.Slot) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, existing := range m.slots {
		if existing.DoctorID == s.DoctorID && existing.Overlaps(s) {
			return domain.ErrSlotTaken
		}
	}
	m.slots[s.ID] = s
	return nil
}

func (m *Memory) ListSlots(_ context.Context, doctorID string, from, to time.Time, onlyFree bool) ([]domain.Slot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := []domain.Slot{}
	for _, s := range m.slots {
		if s.DoctorID != doctorID {
			continue
		}
		if onlyFree && s.Reserved {
			continue
		}
		if !from.IsZero() && s.StartsAt.Before(from) {
			continue
		}
		if !to.IsZero() && s.StartsAt.After(to) {
			continue
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartsAt.Before(out[j].StartsAt) })
	return out, nil
}

func (m *Memory) GetSlot(_ context.Context, slotID string) (domain.Slot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.slots[slotID]
	if !ok {
		return domain.Slot{}, domain.ErrSlotNotFound
	}
	return s, nil
}

func (m *Memory) ReserveSlot(_ context.Context, slotID, appointmentID string) (domain.Slot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.slots[slotID]
	if !ok {
		return domain.Slot{}, domain.ErrSlotNotFound
	}
	if s.Reserved {
		return domain.Slot{}, domain.ErrSlotTaken
	}

	s.Reserved = true
	s.AppointmentID = appointmentID
	m.slots[slotID] = s
	return s, nil
}

func (m *Memory) ReleaseSlot(_ context.Context, slotID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.slots[slotID]
	if !ok {
		return domain.ErrSlotNotFound
	}
	s.Reserved = false
	s.AppointmentID = ""
	m.slots[slotID] = s
	return nil
}

// SeedDoctor inserts a doctor bypassing validation, for provider states.
func (m *Memory) SeedDoctor(d domain.Doctor) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doctors[d.ID] = d
}

// SeedSlot inserts a slot bypassing overlap checks, for provider states.
func (m *Memory) SeedSlot(s domain.Slot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.slots[s.ID] = s
}
