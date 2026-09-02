# syntax=docker/dockerfile:1.7

# Build stage runs on the builder's native architecture and cross-compiles to
# the target platform, which is much faster than emulating amd64 under QEMU on
# an arm64 workstation.
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS build

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

WORKDIR /src

# Dependencies first so the module layer caches across source edits.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags "-s -w \
        -X main.version=${VERSION} \
        -X main.commit=${COMMIT} \
        -X main.date=${BUILD_DATE}" \
      -o /out/loadtest ./cmd/loadtest

# Runtime stage is pinned to linux/amd64 to match the deployment target.
FROM --platform=linux/amd64 gcr.io/distroless/static-debian12:nonroot AS runtime

COPY --from=build /out/loadtest /usr/local/bin/loadtest

USER nonroot:nonroot
EXPOSE 8088

ENTRYPOINT ["/usr/local/bin/loadtest"]
CMD ["-brokers", "kafka:9092", "-sports", "all"]
