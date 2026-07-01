# Build stage.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Copy manifests first so dependency download is cached independently of source
# changes: editing a .go file should not re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO is off so the binary is fully static and can run in a scratch image.
# Trimming paths and stripping symbols keeps the layer small; this binary is
# never debugged in-container.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/ingester ./cmd/ingester

# Runtime stage.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/ingester /usr/local/bin/ingester

# TLS roots come with the distroless base, which the live API needs for HTTPS.
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/ingester"]
