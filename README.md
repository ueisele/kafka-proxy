# Kafka Proxy

Build
```
sbt assembly
```

Run
```
java -Dlog4j.configurationFile=src/main/resources/log4j2.yml -jar target/scala-2.13/scala-tcpsocket-playground-assembly-0.1.jar config/proxy.properties
```
