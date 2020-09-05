.DEFAULT_GOAL := build

NS ?= asos
REGISTRY ?=bigdataengdev.azurecr.io
VERSION ?= latest

IMAGE_NAME ?= brandrecscorer

TAG = $(REGISTRY)/$(NS)/$(IMAGE_NAME):$(VERSION)

build: Dockerfile
	docker build --no-cache -t $(TAG) -f Dockerfile .

push: build
	docker push $(TAG)

release:
	make push -e VERSION=$(VERSION)
