all: subscriptions

test-prog: dev-test/dev-test

SOURCES = $(wildcard *.go) $(wildcard */*.go)

subscriptions: ${SOURCES}
	go build --buildvcs=false .

dev-test/dev-test: ${SOURCES}
	go build --trimpath -o dev-test ./dev-test

clean:
	rm -rf subscriptions dev-test/dev-test

.PHONY: all clean test-prog
