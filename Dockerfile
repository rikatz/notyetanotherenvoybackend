FROM golang:1.26 AS build

WORKDIR /go/src/app
COPY . .

RUN --mount=type=cache,target=/go/pkg/mod/ \
    go mod tidy

RUN CGO_ENABLED=0 go build -o /go/bin/nginx-controller nginxbackend/main.go

FROM docker.io/nginxinc/nginx-unprivileged:1.29

COPY --from=build /go/bin/nginx-controller /
RUN rm -rf /etc/nginx/*
COPY --chown=101 nginxbackend/config/nginx.conf /etc/nginx/nginx.conf
ENTRYPOINT ["/nginx-controller"]