FROM --platform=amd64 python:3.9.16-bullseye

WORKDIR /usr/src/app

COPY . .

RUN pip install --extra-index-url=https://pypi.celonis.cloud/ .

# CMD [ "python", "./pyinsights/log_skeleton/main.py" ]