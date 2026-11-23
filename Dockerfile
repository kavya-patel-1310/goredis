# ---- build ----
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /goredis ./cmd/goredis

# ---- run ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /goredis /goredis
VOLUME /data
EXPOSE 6379 8080
USER nonroot:nonroot
ENTRYPOINT ["/goredis", "-dir", "/data", "-appendonly", "-dashboard", "0.0.0.0:8080"]
