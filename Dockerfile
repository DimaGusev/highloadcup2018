FROM anapsix/alpine-java:8_jdk
WORKDIR /opt
WORKDIR /
ADD data.zip /tmp/data/data.zip
ADD options.txt /tmp/data/options.txt
ENV JAVA_OPTS="-server -Xmx1450m -Xms1450m -Xmn600m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+UseParallelGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
EXPOSE 80
ADD target/accounts-app-1.0.jar /opt/app.jar
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /opt/app.jar" ]