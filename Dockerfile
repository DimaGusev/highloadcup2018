FROM anapsix/alpine-java:8_jdk
WORKDIR /opt
ADD target/accounts-app-1.0.jar app.jar
WORKDIR /
ENV JAVA_OPTS="-server -Xmx1600m -XX:CompileThreshold=1 -XX:+UseParallelGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Duser.timezone=UTC -Dfile.encoding=UTF-8"
EXPOSE 80
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /opt/app.jar" ]