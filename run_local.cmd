mvn clean install -DskipTests=true & docker build . -t hl2-local & docker run --memory-swappiness=0 --name hl2 -p 80:80 -m 2g --memory-swap 2g --rm -it hl2-local
