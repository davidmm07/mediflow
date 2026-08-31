#!/usr/bin/env bash
# End-to-end smoke test against a running stack (`make up`).
#
# It walks the whole distributed flow in one pass: register through
# auth-service, obtain a real Keycloak token, publish a doctor profile and a
# slot, book it through appointment-service (which calls doctor-service
# synchronously), then poll notification-service until the Kafka-driven
# notification shows up. That last step is what proves the async path works —
# nothing in the HTTP responses tells you the event was delivered.
set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:8080}"
KEYCLOAK="${KEYCLOAK:-http://localhost:8081}"
REALM="${REALM:-mediflow}"
CLIENT_ID="${CLIENT_ID:-mediflow-gateway}"

log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$1"; }
fail() { printf '\033[1;31mFAIL: %s\033[0m\n' "$1" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || fail "$1 is required"; }
need curl
need jq

token_for() {
  curl -sS -X POST \
    "${KEYCLOAK}/realms/${REALM}/protocol/openid-connect/token" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -d "client_id=${CLIENT_ID}" \
    -d 'grant_type=password' \
    -d "username=$1" \
    -d "password=$2" | jq -r '.access_token'
}

log "Waiting for the gateway to become healthy"
for _ in $(seq 1 60); do
  if curl -sf "${GATEWAY}/health" >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -sf "${GATEWAY}/health" >/dev/null || fail "gateway never became healthy"

log "Registering a new patient through auth-service"
SUFFIX="$(date +%s)"
PATIENT_USER="smoke.patient.${SUFFIX}"
REGISTER=$(curl -sS -X POST "${GATEWAY}/auth/register" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"${PATIENT_USER}\",\"email\":\"${PATIENT_USER}@mediflow.dev\",\"first_name\":\"Smoke\",\"last_name\":\"Tester\",\"password\":\"smoke-password-123\",\"role\":\"patient\"}")
echo "$REGISTER" | jq -e '.user_id' >/dev/null || fail "registration failed: $REGISTER"
PATIENT_ID=$(echo "$REGISTER" | jq -r '.user_id')
echo "registered patient ${PATIENT_ID}"

log "Obtaining tokens from Keycloak"
DOCTOR_TOKEN=$(token_for "dr.house" "doctor123")
[ "$DOCTOR_TOKEN" != "null" ] || fail "could not authenticate the seeded doctor"
PATIENT_TOKEN=$(token_for "$PATIENT_USER" "smoke-password-123")
[ "$PATIENT_TOKEN" != "null" ] || fail "could not authenticate the new patient"

log "Publishing a doctor profile"
DOCTOR=$(curl -sS -X POST "${GATEWAY}/doctors" \
  -H "Authorization: Bearer ${DOCTOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"full_name":"Gregory House","specialty":"cardiology","license_number":"ES-CARD-99120","bio":"Diagnostic medicine","consultation_fee":75,"languages":["es","en"]}')
DOCTOR_ID=$(echo "$DOCTOR" | jq -r '.id // empty')
if [ -z "$DOCTOR_ID" ]; then
  # Re-running the smoke test hits the one-profile-per-user rule; reuse the
  # existing profile instead of failing.
  DOCTOR_ID=$(curl -sS "${GATEWAY}/doctors?specialty=cardiology" \
    -H "Authorization: Bearer ${DOCTOR_TOKEN}" | jq -r '.doctors[0].id')
fi
[ -n "$DOCTOR_ID" ] && [ "$DOCTOR_ID" != "null" ] || fail "no doctor profile available"
echo "doctor ${DOCTOR_ID}"

log "Opening an availability slot"
STARTS_AT=$(date -u -d '+7 days 09:00' +%Y-%m-%dT%H:00:00Z 2>/dev/null || date -u -v+7d +%Y-%m-%dT09:00:00Z)
ENDS_AT=$(date -u -d '+7 days 09:30' +%Y-%m-%dT%H:30:00Z 2>/dev/null || date -u -v+7d +%Y-%m-%dT09:30:00Z)
SLOT=$(curl -sS -X POST "${GATEWAY}/doctors/${DOCTOR_ID}/slots" \
  -H "Authorization: Bearer ${DOCTOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d "{\"starts_at\":\"${STARTS_AT}\",\"ends_at\":\"${ENDS_AT}\"}")
SLOT_ID=$(echo "$SLOT" | jq -r '.id // empty')
[ -n "$SLOT_ID" ] || fail "could not create a slot: $SLOT"
echo "slot ${SLOT_ID} at ${STARTS_AT}"

log "Booking the appointment (appointment-service -> doctor-service)"
APPOINTMENT=$(curl -sS -X POST "${GATEWAY}/appointments" \
  -H "Authorization: Bearer ${PATIENT_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d "{\"doctor_id\":\"${DOCTOR_ID}\",\"slot_id\":\"${SLOT_ID}\",\"reason\":\"smoke test consultation\"}")
APPOINTMENT_ID=$(echo "$APPOINTMENT" | jq -r '.id // empty')
[ -n "$APPOINTMENT_ID" ] || fail "booking failed: $APPOINTMENT"
echo "appointment ${APPOINTMENT_ID} confirmed"

log "Confirming the slot is no longer offered"
STILL_FREE=$(curl -sS "${GATEWAY}/doctors/${DOCTOR_ID}/slots?available=true" \
  -H "Authorization: Bearer ${PATIENT_TOKEN}" | jq -r "[.slots[]? | select(.id == \"${SLOT_ID}\")] | length")
[ "$STILL_FREE" = "0" ] || fail "slot ${SLOT_ID} is still advertised as available"

log "Waiting for the Kafka-driven notification"
for _ in $(seq 1 30); do
  COUNT=$(curl -sS "${GATEWAY}/notifications/me" \
    -H "Authorization: Bearer ${PATIENT_TOKEN}" |
    jq -r "[.notifications[]? | select(.source_id == \"${APPOINTMENT_ID}\")] | length")
  [ "$COUNT" != "0" ] && break
  sleep 2
done
[ "${COUNT:-0}" != "0" ] || fail "no notification arrived for appointment ${APPOINTMENT_ID}"

log "Cancelling, then confirming the slot is released"
curl -sS -X POST "${GATEWAY}/appointments/${APPOINTMENT_ID}/cancel" \
  -H "Authorization: Bearer ${PATIENT_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"reason":"smoke test cleanup"}' | jq -e '.status == "cancelled"' >/dev/null \
  || fail "cancellation did not return a cancelled appointment"

RELEASED=$(curl -sS "${GATEWAY}/doctors/${DOCTOR_ID}/slots?available=true" \
  -H "Authorization: Bearer ${PATIENT_TOKEN}" | jq -r "[.slots[]? | select(.id == \"${SLOT_ID}\")] | length")
[ "$RELEASED" = "1" ] || fail "slot ${SLOT_ID} was not released after cancellation"

printf '\n\033[1;32mSmoke test passed: registration, booking, events and compensation all work.\033[0m\n'
