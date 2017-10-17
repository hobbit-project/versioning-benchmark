
default: build dockerize

build:
	mvn clean package -U -Dmaven.test.skip=true

dockerize:
	docker build -f docker/versioningbenchmarkcontroller.docker -t versioningbenchmarkcontroller .
	docker build -f docker/versioningdatagenerator.docker -t versioningdatagenerator .
	docker build -f docker/versioningtaskgenerator.docker -t versioningtaskgenerator .
	docker build -f docker/versioningevaluationmodule.docker -t versioningevaluationmodule .
	docker build -f docker/versioningvirtuososystemadapter.docker -t versioningsystem .
