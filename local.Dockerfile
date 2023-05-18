FROM  python:3.9.16-bullseye

WORKDIR /usr/src/app

RUN apt-get update && apt-get -y install dos2unix
RUN mkdir ~/.streamlit
RUN echo "[server]\nport=8501" > ~/.streamlit/config.toml#
EXPOSE 8501
EXPOSE 80
EXPOSE 443

COPY . .
RUN pip install --extra-index-url=https://pypi.celonis.cloud/ .
RUN dos2unix ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

ENTRYPOINT [ "./entrypoint.sh" ]
