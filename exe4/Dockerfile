FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    openjdk-11-jdk-headless \
    procps \
    git \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN apt-get install -y libffi-dev

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY profiles.yml /root/.dbt/profiles.yml
COPY . /app

RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["tail", "-f", "/dev/null"]
