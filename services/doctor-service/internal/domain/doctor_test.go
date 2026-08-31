package domain_test

import (
	"testing"
	"time"

	"github.com/davidmm07/mediflow/services/doctor-service/internal/domain"
	"github.com/stretchr/testify/require"
)

func slotAt(startHour, durationMinutes int) domain.Slot {
	start := time.Date(2026, 9, 14, startHour, 0, 0, 0, time.UTC)
	return domain.Slot{
		StartsAt: start,
		EndsAt:   start.Add(time.Duration(durationMinutes) * time.Minute),
	}
}

func TestSlotValidate(t *testing.T) {
	cases := map[string]struct {
		slot    domain.Slot
		wantErr bool
	}{
		"30 minute consultation": {slotAt(9, 30), false},
		"minimum length":         {slotAt(9, 10), false},
		"maximum length":         {slotAt(9, 180), false},
		"too short":              {slotAt(9, 5), true},
		"too long":               {slotAt(9, 181), true},
		"zero length":            {slotAt(9, 0), true},
		"missing timestamps":     {domain.Slot{}, true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.slot.Validate()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestSlotValidateRejectsInvertedRange(t *testing.T) {
	slot := slotAt(9, 30)
	slot.StartsAt, slot.EndsAt = slot.EndsAt, slot.StartsAt

	require.Error(t, slot.Validate())
}

func TestSlotOverlaps(t *testing.T) {
	base := slotAt(9, 60) // 09:00 - 10:00

	cases := map[string]struct {
		other domain.Slot
		want  bool
	}{
		"identical":          {slotAt(9, 60), true},
		"starts inside":      {slotAt(9, 30), true},
		"contained":          {domain.Slot{StartsAt: base.StartsAt.Add(15 * time.Minute), EndsAt: base.StartsAt.Add(45 * time.Minute)}, true},
		"back to back after": {slotAt(10, 60), false},
		"clearly later":      {slotAt(14, 60), false},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, base.Overlaps(tc.other))
			// Overlap must be symmetric, otherwise insertion order would
			// decide whether a double booking is caught.
			require.Equal(t, tc.want, tc.other.Overlaps(base))
		})
	}
}

func TestDoctorValidate(t *testing.T) {
	valid := domain.Doctor{
		FullName:      "Gregory House",
		Specialty:     "cardiology",
		LicenseNumber: "ES-CARD-99120",
	}
	require.NoError(t, valid.Validate())

	missingLicense := valid
	missingLicense.LicenseNumber = ""
	require.Error(t, missingLicense.Validate())

	negativeFee := valid
	negativeFee.ConsultationFee = -1
	require.Error(t, negativeFee.Validate())
}
