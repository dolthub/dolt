FROM zaqwsx_ipfs-test-img

RUN ipfs init -b=1024
ADD . /tmp/id
RUN mv -f /tmp/id/config /root/.ipfs/config
RUN ipfs id

ENV IPFS_PROF true
ENV IPFS_LOGGING_FMT nocolor

EXPOSE 4011 4012/udp
