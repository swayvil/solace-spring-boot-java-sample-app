# Solace Spring Boot Sample - Java

This is a simple sample project to demonstrate how to start a publisher and consumer on the same connection.

Please refer to the [solace-spring-boot](https://github.com/SolaceProducts/solace-spring-boot) project for more detail.

## Running the Sample
1. Configure the [application.properties](https://github.com/swayvil/solace-spring-boot-java-sample-app/src/main/resources/application.properties)
2. Create a queue "demo-sample" that subscribres to "demo/sample"
3. The simplest way to run the sample is from the project root folder using maven. For example:
```shell script
mvn spring-boot:run
```
