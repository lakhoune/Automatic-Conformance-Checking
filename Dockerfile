FROM --platform=amd64 python:3.9.16-bullseye

WORKDIR /usr/src/app

COPY . .

RUN pip install --extra-index-url=https://pypi.celonis.cloud/ .

RUN echo "[server]\nport=8080" > ~/.streamlit/config.toml
EXPOSE 8080

CMD [ "streamlit", "run app.py" ]