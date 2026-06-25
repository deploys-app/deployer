FROM registry.deploys.app/public/builder

ENV CGO_ENABLED=0
ENV GOCACHE=/root/.cache/go-build
ENV GOMODCACHE=/go/pkg/mod

WORKDIR /workspace

ADD .tool-versions .
RUN asdf install

ADD go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
ADD . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o .build/deployer -ldflags "-w -s" .

FROM gcr.io/distroless/static

WORKDIR /app

COPY --from=0 --link /workspace/.build/* ./
ENTRYPOINT ["/app/deployer"]
