# ESC-Streaming-Architectures Project

This project is the main part of my thesis about developing and evaluating Streaming Architectures. In here are the Producers and Consumer for the different streaming technologies:
- Apache Kafka
- Kinesis Data Steams
- MongoDB Change Streams

The other projects and parts are:
- [1Zero64/ESC-Streaming-Architectures-Thesis-Materializer](https://github.com/1Zero64/ESC-Streaming-Architectures-Thesis-Materializer) developed in Go
- [1Zero64/ESC-Streaming-Architectures-Thesis-Benchmark](https://github.com/1Zero64/ESC-Streaming-Architectures-Thesis-Benchmark) developed in Python

It uses Quarkus, the Supersonic Subatomic Java Framework. If you want to learn more about Quarkus, please visit its website: https://quarkus.io/ .

## The architecture
![Architecture](architecture.png)

## Setup

For setting up the project, so it can work on your computer, read the [Setup Manual](Setup-Manual.md).

## Running the application in dev mode

You can run the application in dev mode that enables live coding using:
```shell script
quarkus dev
```
or
```shell script
./mvnw compile quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8080/q/dev/.

## Console
In this project a simple [HTML-console](console.html) is available with some GET-Requests to the Quarkus application. YOu can use it in the browser and execute these requests.

Here is the link to open it via IntelliJ IDEA:

http://localhost:63342/ESC-Streaming-Architectures-Thesis/code-with-quarkus/console.html

> **_NOTE:_**  The application must be running in order to send Requests to REST API.

## Packaging and running the application

The application can be packaged using:
```shell script
./mvnw package
```
It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:
```shell script
./mvnw package -Dquarkus.package.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

## Creating a native executable

You can create a native executable using: 
```shell script
./mvnw package -Pnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using: 
```shell script
./mvnw package -Pnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/code-with-quarkus-1.0.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult https://quarkus.io/guides/maven-tooling.
