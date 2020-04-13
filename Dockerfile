FROM golang:1.14.2-buster as builder
WORKDIR /root/building/go
COPY ./go/ .
ENV GOFLAGS="-mod=readonly"
RUN go build -o dolt ./cmd/dolt

FROM ubuntu:18.04
COPY --from=builder /root/building/go/dolt /usr/local/bin/dolt
ENTRYPOINT [ "/usr/local/bin/dolt" ]
