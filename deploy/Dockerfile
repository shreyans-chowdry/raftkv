# Build all RaftKV binaries, then copy them into a small runtime image.
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/...

FROM alpine:3.20
RUN apk add --no-cache curl
COPY --from=build /out/ /usr/local/bin/
ENTRYPOINT ["raftkv-server"]
