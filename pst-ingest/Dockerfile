FROM openjdk:8
VOLUME /tmp
ADD target/scala-2.12/pst-ingest-*.jar app.jar
CMD mkdir /promotions
#ADD src/test/resources/promotions /promotions
RUN sh -c 'touch /app.jar'
ENTRYPOINT ["java", "-Xmx1g", "-Xms512m", "-Djava.security.egd=file:/dev/./urandom", "-jar", "app.jar"]