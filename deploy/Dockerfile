# Single multi-stage Dockerfile reused by every service: the SERVICE build arg
# selects which module inside the repository gets compiled, so there is one
# build recipe to maintain instead of six near-identical ones.
FROM golang:1.25-alpine AS build

ARG SERVICE
WORKDIR /src

RUN apk add --no-cache git

# No go.work here, on purpose. Every service's go.mod already carries a replace
# directive pointing at the shared module, so an image builds from the modules
# themselves and does not care what the developer workspace happens to list.
# Copying the workspace file made every module in it a build dependency of all
# six images, so adding a dev-only tool broke builds that never used it.
ENV GOWORK=off

COPY common ./common
COPY gateway ./gateway
COPY services ./services

RUN cd "${SERVICE}" && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/app .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app"]
