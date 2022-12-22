FROM --platform=amd64 python:3.9.16-bullseye

WORKDIR /usr/src/app

RUN mkdir ~/.streamlit
RUN echo "[server]\nport=8080" > ~/.streamlit/config.toml#
EXPOSE 8080

COPY . .
RUN pip install --extra-index-url=https://pypi.celonis.cloud/ .

RUN chmod +x ./entrypoint.sh

ENTRYPOINT [ "./entrypoint.sh" ]
