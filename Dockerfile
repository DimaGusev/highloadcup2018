FROM anapsix/alpine-java:8_jdk
WORKDIR /opt
WORKDIR /
ENV JAVA_OPTS="-server -Xmx1620m -Xms1620m -Xmn500m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+UseParallelGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
EXPOSE 80
ADD target/accounts-app-1.0.jar /opt/app.jar
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /opt/app.jar" ]