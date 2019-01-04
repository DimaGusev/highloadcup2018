FROM anapsix/alpine-java:8_jdk
WORKDIR /opt
WORKDIR /
ENV JAVA_OPTS="-server -Xmx1680m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+UseParallelGC"
EXPOSE 80
ADD target/accounts-app-1.0.jar /opt/app.jar
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /opt/app.jar" ]