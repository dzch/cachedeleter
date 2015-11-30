# Cachedeleter

A delay deleter for redis cache cluster which is built with twemproxy.

# Build

    go get github.com/dzch/cachedeleter
    cd ${GOPATH}/src/github.com/dzch/cachedeleter
	go build

# Config

please see example/conf/cachedeleter.yaml

# Run

    cp cachedeleter_bin example
    ./cachedeleter_bin
