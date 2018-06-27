.PHONY: all install list test version ci-build ci-run ci-test ci-remove ci-push-tag ci-push-latest ci-login


PACKAGE := $(shell grep '^PACKAGE =' setup.py | cut -d "'" -f2)
VERSION := $(shell head -n 1 $(PACKAGE)/VERSION)

NAME   := measure
ORG    := openknowledge
REPO   := ${ORG}/${NAME}
TAG    := $(shell git log -1 --pretty=format:"%h")
IMG    := ${REPO}:${TAG}
LATEST := ${REPO}:latest


all: list

install:
	pip install --upgrade -e .[develop]

list:
	@grep '^\.PHONY' Makefile | cut -d' ' -f2- | tr ' ' '\n'

test:
	tox

version:
	@echo $(VERSION)

ci-build:
	docker build --cache-from ${LATEST} -t ${IMG} -t ${LATEST} .

ci-run:
	docker run ${RUN_ARGS} --name ${NAME} -d ${LATEST}

ci-test:
	docker ps | grep latest
	docker exec ${NAME} npm test

ci-remove:
	docker rm -f ${NAME}

ci-push: ci-login ci-build
	docker push ${IMG}
	docker push ${LATEST}

ci-push-tag: ci-login
	docker build -t ${REPO}:${TAG} .
	docker push ${REPO}:${TAG}

ci-login:
	docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}
