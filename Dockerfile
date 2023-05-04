FROM golang:1.19-bullseye AS builder

RUN apk update && apk add --no-cache git ca-certificates make build-base && update-ca-certificates

WORKDIR /go/src/index-observer
COPY go.* ./
RUN go mod download
COPY . .
RUN go build -o /index-observer

FROM golang:alpine
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /index-observer /index-observer
ENTRYPOINT ["/index-observer"]
