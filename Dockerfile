FROM frictionlessdata/datapackage-pipelines:latest

ADD . /app

WORKDIR /app
RUN pip install .
RUN apk add --update postgresql-client

WORKDIR /app/projects

CMD ["server"]
