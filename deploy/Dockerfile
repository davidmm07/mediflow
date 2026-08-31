# Single multi-stage Dockerfile reused by every service: the SERVICE build arg
# selects which module inside the Go workspace gets compiled, so there is one
# build recipe to maintain instead of six near-identical ones.
FROM golang:1.25-alpine AS build

ARG SERVICE
WORKDIR /src

RUN apk add --no-cache git

COPY go.work ./
COPY common ./common
COPY gateway ./gateway
COPY services ./services

RUN go work sync || true
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/app ./${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app"]
