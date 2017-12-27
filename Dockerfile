FROM sociomantictsunami/dlang:xenial-v3 as builder
COPY . /project/
WORKDIR /project/
RUN docker/build
RUN make pkg DMD=dmd1 F=production DVER=1

FROM ubuntu:xenial
COPY --from=builder /project/build/production/pkg/dhtnode-d1*.deb /packages/
RUN apt update
RUN mkdir -p /srv/dhtnode/dhtnode-0 && apt install -y /packages/*.deb
COPY ./doc/etc/dht.config.ini /srv/dhtnode/dhtnode-0/etc/config.ini
WORKDIR /srv/dhtnode/dhtnode-0
ENTRYPOINT [ "dhtnode-d1", "-c", "/srv/dhtnode/dhtnode-0/etc/config.ini" ]
