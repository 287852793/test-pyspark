include version.Makefile
export PROJROOT=$(shell pwd)

.PHONY: ready
ready:
	./download_source.sh

.PHONY: build
build: ready
	docker build \
		--build-arg SOURCE_SPARK=${SOURCE_SPARK} \
		--build-arg IMAGE_BASE=${IMAGE_BASE} \
		-t ${DEV_IMAGE}:${DEV_VERSION} .

.PHONY: run
run:
	docker run -d \
		-v ${PWD}/code/:/code/ \
		-v ${PWD}/volumes/hosts:/etc/hosts \
		--rm \
		-p 8888:8888 \
		-p 4040:4040 \
		--name pyspark \
		${DEV_IMAGE}:${DEV_VERSION}

.PHONY: stop
stop:
	docker stop pyspark

.PHONY: test
test:
	docker run --rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v /usr/bin/docker:/usr/bin/docker \
		-v ${PWD}/code/:/code/ \
		-v ${PWD}/volumes/hadoop-conf:/hadoop-conf \
		-v ${PWD}/entrypoint_for_test.sh:/entrypoint_for_test.sh \
		--rm \
		-p 4040:4040 \
		--name pyspark \
		--net=host \
		-w /installs/spark/spark-3.3.1-bin-hadoop3/bin \
		--entrypoint /entrypoint_for_test.sh \
		-it ${DEV_IMAGE}:${DEV_VERSION}

.PHONY: login
login:
	docker login --username=southgis-dl --password=Dl123456 172.20.20.187

