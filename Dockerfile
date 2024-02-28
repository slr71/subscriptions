FROM golang:1.22 as build-root

WORKDIR /go/src/github.com/cyverse-de/subscriptions
COPY . .

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

RUN go build --buildvcs=false .
RUN go clean -cache -modcache
RUN cp ./subscriptions /bin/subscriptions

ENTRYPOINT ["subscriptions"]

EXPOSE 60000
