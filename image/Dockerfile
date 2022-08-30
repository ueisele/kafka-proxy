FROM docker.io/ueisele/openjdk-jdk:zulu17.0.4 as build
LABEL maintainer="code@uweeisele.eu"

USER root

RUN microdnf install -y git \
    && microdnf clean all \
    && rm -rf /tmp/*

USER appuser

COPY --chown=appuser:appuser ./ /home/appuser/kafka-proxy/
WORKDIR /home/appuser/kafka-proxy/

RUN ./sbtx clean package packCopyDependencies
RUN mkdir -p dist/libs dist/dependant-libs \
    && cp target/libs/* dist/dependant-libs/ \
    && mv dist/dependant-libs/kafka-proxy* dist/libs


FROM docker.io/ueisele/openjdk-jre:zulu17.0.4
LABEL maintainer="code@uweeisele.eu"

USER root

ARG GODUB_VERSION=0.1.0
LABEL godub.version=${GODUB_VERSION}
RUN curl -s -L https://github.com/ueisele/go-docker-utils/releases/download/${GODUB_VERSION}/godub-${GODUB_VERSION}-linux-amd64.tar.gz \
    | tar xvz -C /usr/local/bin

RUN mkdir -p /opt/uweeisele/kafka-proxy /opt/uweeisele/kafka-proxy/config /opt/uweeisele/kafka-proxy/secrets /opt/uweeisele/kafka-proxy/logs \
    && chown appuser:appuser -R /opt/uweeisele/kafka-proxy

VOLUME ["/opt/kafka-proxy/secrets"]

USER appuser
WORKDIR /home/appuser

COPY --from=build --chown=appuser:appuser /home/appuser/kafka-proxy/dist/dependant-libs /opt/uweeisele/kafka-proxy/libs
COPY --from=build --chown=appuser:appuser /home/appuser/kafka-proxy/dist/libs /opt/uweeisele/kafka-proxy/libs

COPY --from=build --chown=appuser:appuser /home/appuser/kafka-proxy/LICENSE /opt/uweeisele/kafka-proxy
COPY --from=build --chown=appuser:appuser /home/appuser/kafka-proxy/NOTICE.md /opt/uweeisele/kafka-proxy

COPY --from=build --chown=appuser:appuser /home/appuser/kafka-proxy/bin /opt/uweeisele/kafka-proxy/bin
ENV PATH=/opt/uweeisele/kafka-proxy/bin:${PATH}

COPY --chown=appuser:appuser image/include/opt/uweeisele/kafka-proxy/docker /opt/uweeisele/kafka-proxy/docker

EXPOSE 9092

CMD ["/opt/uweeisele/kafka-proxy/docker/run"]
