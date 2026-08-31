package store

import (
	"context"
	"sync"

	"github.com/davidmm07/mediflow/services/patient-service/internal/domain"
)

// Memory is an in-memory Store for unit tests.
type Memory struct {
	mu       sync.RWMutex
	patients map[string]domain.Patient
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{patients: make(map[string]domain.Patient)}
}

func (m *Memory) Create(_ context.Context, p domain.Patient) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, existing := range m.patients {
		if existing.KeycloakUserID == p.KeycloakUserID {
			return domain.ErrDuplicate
		}
	}
	m.patients[p.ID] = p
	return nil
}

func (m *Memory) GetByUserID(_ context.Context, keycloakUserID string) (domain.Patient, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, p := range m.patients {
		if p.KeycloakUserID == keycloakUserID {
			return p, nil
		}
	}
	return domain.Patient{}, domain.ErrNotFound
}

func (m *Memory) Update(_ context.Context, p domain.Patient) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.patients[p.ID]; !ok {
		return domain.ErrNotFound
	}
	m.patients[p.ID] = p
	return nil
}

// All returns every stored patient, for assertions in tests.
func (m *Memory) All() []domain.Patient {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]domain.Patient, 0, len(m.patients))
	for _, p := range m.patients {
		out = append(out, p)
	}
	return out
}
