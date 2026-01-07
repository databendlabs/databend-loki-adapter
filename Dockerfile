FROM scratch
ARG TARGETARCH
COPY artifacts/${TARGETARCH}/databend-loki-adapter /databend-loki-adapter
ENTRYPOINT ["/databend-loki-adapter"]
