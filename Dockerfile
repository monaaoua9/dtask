FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/dtaskd ./cmd/dtaskd \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/dtaskctl ./cmd/dtaskctl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/dtaskd /out/dtaskctl /usr/local/bin/
VOLUME /data
EXPOSE 7000 9090
ENTRYPOINT ["/usr/local/bin/dtaskd"]
CMD ["--data-dir=/data"]
