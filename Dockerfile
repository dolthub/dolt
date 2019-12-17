FROM golang:1.13.5-buster as builder
WORKDIR /root/building/go
COPY ./go/ .
RUN go mod vendor
RUN go build -mod=vendor -o dolt ./cmd/dolt

FROM ubuntu:18.04
COPY --from=builder /root/building/go/dolt /usr/local/bin/dolt
ENTRYPOINT [ "/usr/local/bin/dolt" ]
