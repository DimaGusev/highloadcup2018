mvn clean install -DskipTests=true & docker build . -t hl2 & docker tag hl2 stor.highloadcup.ru/accounts/quick_numbat & docker push stor.highloadcup.ru/accounts/quick_numbat
