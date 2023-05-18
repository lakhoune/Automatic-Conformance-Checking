FROM  python:3.9.16-bullseye

ENV PORT 8501

WORKDIR /usr/src/app

RUN apt-get update && apt-get -y install dos2unix
EXPOSE $PORT
EXPOSE 80
EXPOSE 443

COPY . .
RUN pip install --extra-index-url=https://pypi.celonis.cloud/ .
RUN dos2unix ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

ENTRYPOINT [ "./entrypoint.sh" ]
