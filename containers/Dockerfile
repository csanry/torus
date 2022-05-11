# alternative 

FROM ubuntu:20.04

EXPOSE 9000

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-dev pip wget htop build-essential make && \
    rm -rf /var/lib/apt/lists/* 

WORKDIR /usr/

COPY .. src/

# ENTRYPOINT ["mysql"]


