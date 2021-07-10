FROM maven:3.8.1-openjdk-11-slim AS builder
ENV BT_OPTS="-Xms1024M -Xmx2048M -Xss64M -XX:MaxMetaspaceSize=2048M"
COPY . /usr/local/var/src/NEXMarkStream/
WORKDIR /usr/local/var/src/NEXMarkStream/
RUN mvn install

FROM openjdk:18-slim
WORKDIR /NEXMarkStream
COPY --from=builder /usr/local/var/src/NEXMarkStream/target/NEXMarkStream-jar-with-dependencies.jar .
CMD ["java", "-Xms1g", "-cp", "NEXMarkStream-jar-with-dependencies.jar","edu.illinois.sketchbench.datagen.NEXMarkGenerator"]
