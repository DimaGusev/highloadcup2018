FROM anapsix/alpine-java:8_jdk
WORKDIR /opt
WORKDIR /
ADD data.zip /tmp/data/data.zip
ADD options.txt /tmp/data/options.txt
ENV JAVA_OPTS="-server -XX:GCTimeRatio=1000 -Xmx1180m -Xms1180m -Xmn600m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+UseSerialGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:CompileThreshold=1"
EXPOSE 80
ADD target/accounts-app-1.0.jar /opt/app.jar
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /opt/app.jar" ]