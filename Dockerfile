FROM anapsix/alpine-java:8_jdk
WORKDIR /
ENV JAVA_OPTS="-server -XX:GCTimeRatio=1000 -Xmx1165m -Xms1165m -Xmn600m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+UseSerialGC -XX:CompileThreshold=1"
EXPOSE 80
ADD target/accounts-app-1.0.jar /opt/app.jar
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /opt/app.jar" ]