FROM adoptopenjdk/openjdk11:alpine
WORKDIR /opt
ADD target/accounts-app-1.0.jar app.jar
WORKDIR /
ADD data.zip /tmp/data/data.zip
ENV JAVA_OPTS="-server -Xmx1500m -XX:CompileThreshold=1  -XX:+UseG1GC -XX:MaxGCPauseMillis=50 -Duser.timezone=UTC -Dfile.encoding=UTF-8"
EXPOSE 80
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /opt/app.jar" ]