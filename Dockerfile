FROM frictionlessdata/datapackage-pipelines:1.7.1

RUN apk add --update postgresql-client

ADD . /app

WORKDIR /app
RUN pip install .

WORKDIR /app/projects

CMD ["server"]
