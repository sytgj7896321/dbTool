FROM golang:1.20 as builder

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPROXY='https://goproxy.io,direct'

WORKDIR /build

COPY ./ ./

RUN go build -o dbtool ./

FROM alpine:3.17

LABEL version="1.1.0" maintainer=yi-tao.shi@hp.com

COPY --from=builder /build/dbtool /

ENTRYPOINT ["/dbtool"]
