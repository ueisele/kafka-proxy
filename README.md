# PoC: Kafka Proxy

Build
```
sbt +assembly
```

Run
```
java -Dlog4j.configurationFile=src/main/resources/log4j2.yml -jar target/scala-3.0.0-RC1/kafka-proxy-assembly-0.1.jar config/proxy.properties
```

## SSL

https://docs.confluent.io/platform/current/security/security_tutorial.html#generating-keys-certs

```
mkdir -p target/ssl
```

Generate the keys and certificates
```
keytool -keystore target/ssl/kafka.server.keystore.jks -alias localhost -keyalg RSA -validity 360 -genkeypair -storepass changeit -dname "cn=Kafka Broker, ou=TC, o=Novatec, c=DE" -ext SAN=IP:127.0.0.1,DNS:localhost,DNS:vkafka
```

Create your own Certificate Authority (CA)
```
openssl req -new -nodes -x509 -keyout target/ssl/ca-key -out target/ssl/ca-cert -days 365 -subj "/C=DE/ST=Hessen/L=Frankfurt/O=Novatec/CN=Novatec CA"
keytool -keystore target/ssl/kafka.server.truststore.jks -alias CARoot -importcert -storepass changeit -file target/ssl/ca-cert
keytool -keystore target/ssl/kafka.client.truststore.jks -alias CARoot -importcert -storepass changeit -file target/ssl/ca-cert
```

Sign the certificate
```
keytool -keystore target/ssl/kafka.server.keystore.jks -storepass changeit -alias localhost -certreq -file target/ssl/server-certreq -ext SAN=IP:127.0.0.1,DNS:localhost,DNS:vkafka
openssl x509 -req -CA target/ssl/ca-cert -CAkey target/ssl/ca-key -in target/ssl/server-certreq -out target/ssl/server-certsigned -days 365 -CAcreateserial -extfile <(printf "subjectAltName = IP:127.0.0.1, DNS:localhost, DNS:vkafka")
keytool -keystore target/ssl/kafka.server.keystore.jks -alias CARoot -importcert -file target/ssl/ca-cert
keytool -keystore target/ssl/kafka.server.keystore.jks -alias localhost -importcert -file target/ssl/server-certsigned
```
Hint: Sans are only added during the sign request

proxy.properties
```
listeners=SSL://0.0.0.0:9092,0.0.0.0:9093
ssl.truststore.location=target/ssl/kafka.server.truststore.jks
ssl.truststore.password=changeit
ssl.keystore.location=target/ssl/kafka.server.keystore.jks
ssl.keystore.password=changeit
```

Check
```
openssl s_client -CAfile target/ssl/ca-cert -showcerts -servername localhost -connect localhost:9092
```

Show Certificate
```
cat target/ssl/server-certsigned | openssl x509 -inform pem -noout -text
```

## SBT Wrapper

curl -Ls https://git.io/sbt > sbtx && chmod 0755 sbtx