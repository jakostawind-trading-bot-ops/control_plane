# syntax=docker/dockerfile:1

ARG GO_VERSION=1.27.2
FROM golang:${GO_VERSION}-alpine AS base

WORKDIR /app
ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local

COPY go.mod go.sum* ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

FROM base AS dev

EXPOSE 8000
CMD ["sh", "-c", "go build -o /tmp/cp_server ./cmd/api && exec /tmp/cp_server"]

FROM base AS build

RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/cp_server ./cmd/api

FROM gcr.io/distroless/static-debian12:nonroot AS runtime

WORKDIR /app
COPY --from=build /out/cp_server ./cp_server

USER nonroot:nonroot
EXPOSE 8000
ENTRYPOINT ["/app/cp_server"]
