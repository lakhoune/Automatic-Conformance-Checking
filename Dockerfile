FROM mambaorg/micromamba:0.15.3
USER root
RUN apt-get update && DEBIAN_FRONTEND=“noninteractive” apt-get install -y --no-install-recommends \
       nginx \
       python \
       pip \
       ca-certificates \
       apache2-utils \
       certbot \
       python3-certbot-nginx \
       sudo \
       cifs-utils \
       git \
       && \
    rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get -y install cron
RUN mkdir /usr/src/app
RUN chmod -R 777 /usr/src/app
WORKDIR /usr/src/app

COPY . .

RUN pip install --extra-index-url=https://pypi.celonis.cloud/ -r requirements.txt


COPY nginx.conf /etc/nginx/nginx.conf

USER root
RUN chmod a+x run.sh
RUN sed -i -e 's/\r$//' run.sh
CMD ["./run.sh"]

