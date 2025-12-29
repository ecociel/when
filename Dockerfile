FROM golang:1.24 AS builder

ARG APP

WORKDIR /build

COPY . .

RUN go build -o app cmd/${APP}/main.go

CMD ["/build/app"]
