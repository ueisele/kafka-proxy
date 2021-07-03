FROM registry.access.redhat.com/ubi8/ubi-minimal:8.4 as build

LABEL maintainer="code@uweeisele.eu"

# This affects how strings in Java class files are interpreted. 
# We want UTF-8 and this is the only locale in the base image that supports it
ENV LANG="C.UTF-8"

# Zulu openJDK
ENV ZULU_OPENJDK="zulu16-jdk-headless"
ENV JAVA_HOME="/usr/lib/jvm/zulu16"

RUN microdnf install -y dnf \
    && dnf update -y \
    # https://docs.azul.com/zulu/zuludocs/ZuluUserGuide/PrepareZuluPlatform/AttachYumRepositoryRHEL-SLES-OracleLinuxSys.htm
    && rpm --import https://www.azul.com/files/0xB1998361219BD9C9.txt \
    && dnf -y install https://cdn.azul.com/zulu/bin/zulu-repo-1.0.0-1.noarch.rpm \
    && dnf -y install ${ZULU_OPENJDK} \
    && dnf clean all \
    && rm -rf /tmp/*

WORKDIR /work

COPY ./ ./

RUN ./sbtx +clean +assembly
ARG SCALA_VERSION=2.13
RUN cp target/scala-${SCALA_VERSION}/kafka-proxy-assembly-*.jar kafka-proxy.jar

FROM registry.access.redhat.com/ubi8/ubi-minimal:8.4

LABEL maintainer="code@uweeisele.eu"

# This affects how strings in Java class files are interpreted.  We want UTF-8 and this is the only locale in the
# base image that supports it
ENV LANG="C.UTF-8"

# Zulu openJDK
ENV ZULU_OPENJDK="zulu16-jre-headless"
ENV JAVA_HOME="/usr/lib/jvm/zulu16"

ENV PYTHON_VERSION="39-3.9.2"

RUN microdnf install -y dnf \
    && dnf update -y \
    && dnf install -y git python${PYTHON_VERSION} openssl wget tar procps krb5-workstation iputils hostname \
    # https://docs.azul.com/zulu/zuludocs/ZuluUserGuide/PrepareZuluPlatform/AttachYumRepositoryRHEL-SLES-OracleLinuxSys.htm
    && rpm --import https://www.azul.com/files/0xB1998361219BD9C9.txt \
    && dnf -y install https://cdn.azul.com/zulu/bin/zulu-repo-1.0.0-1.noarch.rpm \
    && dnf -y install ${ZULU_OPENJDK} \
    && alternatives --set python /usr/bin/python3 \
    && python3 -m pip install --upgrade pip setuptools \
    && python3 -m pip install --prefer-binary --prefix=/usr/local --upgrade 'git+https://github.com/confluentinc/confluent-docker-utils@v0.0.45' \
    && dnf remove -y git \
    && dnf clean all \
    && rm -rf /tmp/* \
    && useradd --no-log-init --create-home --shell /bin/bash appuser \
    && mkdir -p /opt/kafka-proxy /opt/kafka-proxy/bin /opt/kafka-proxy/config /opt/kafka-proxy/libs \
    && chown appuser:appuser -R /opt/kafka-proxy

USER appuser
WORKDIR /home/appuser

RUN mkdir -p /opt/kafka-proxy/secrets
VOLUME ["/opt/kafka-proxy/secrets"]

ENV PATH=/opt/kafka-proxy/bin:${PATH}

COPY --chown=appuser:appuser bin /opt/kafka-proxy/bin
COPY --chown=appuser:appuser image/include/opt/kafka-proxy/docker /opt/kafka-proxy/docker

COPY --from=build --chown=appuser:appuser /work/kafka-proxy.jar /opt/kafka-proxy/libs

CMD ["/opt/kafka-proxy/docker/run"]