FROM golang:latest AS build

ENV NOMS_SRC=$GOPATH/src/github.com/attic-labs/noms
ENV CGO_ENABLED=0
ENV GOOS=linux

RUN mkdir -pv $NOMS_SRC
COPY . ${NOMS_SRC}
RUN go install -v github.com/attic-labs/noms/cmd/noms
RUN cp $GOPATH/bin/noms /bin/noms

FROM alpine:latest

COPY --from=build /bin/noms /bin/noms

VOLUME /data
EXPOSE 8000

ENV NOMS_VERSION_NEXT=1
ENTRYPOINT [ "noms" ]

CMD ["serve", "/data"]