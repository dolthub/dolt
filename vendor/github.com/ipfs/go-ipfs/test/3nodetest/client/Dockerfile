FROM zaqwsx_ipfs-test-img

RUN ipfs init -b=1024
ADD . /tmp/id
RUN mv -f /tmp/id/config /root/.ipfs/config
RUN ipfs id

EXPOSE 4031 4032/udp

ENV IPFS_PROF true
ENV IPFS_LOGGING_FMT nocolor

ENTRYPOINT ["/bin/bash"]
CMD ["/tmp/id/run.sh"]
