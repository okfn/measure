FROM frictionlessdata/datapackage-pipelines:latest

ADD . /app

WORKDIR /app
RUN pip install .

WORKDIR /app/pipelines

CMD ["server"]
