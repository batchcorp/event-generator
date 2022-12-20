# This Dockerfile utilizes a multi-stage builds
ARG ALPINE_VERSION=3.13

FROM golang:1.15-alpine$ALPINE_VERSION AS builder

# Install necessary build tools
RUN apk --update add make bash curl git

# Switch workdir, otherwise we end up in /go (default)
WORKDIR /

# Copy everything into build container
COPY . .

# Build the application
RUN make build/linux

# Now in 2nd build stage
FROM library/alpine:$ALPINE_VERSION

# Necessary depedencies
RUN apk --update add bash curl ca-certificates && update-ca-certificates

# Install binary
COPY --from=builder /build/event-generator-linux /event-generator-linux
COPY --from=builder /docker-entrypoint.sh /docker-entrypoint.sh
