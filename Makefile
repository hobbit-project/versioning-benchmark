
default: build dockerize

build:
	mvn clean package -U -Dmaven.test.skip=true

dockerize:
	docker build -f docker/versioningbenchmarkcontroller.docker -t git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller .
	docker build -f docker/versioningdatagenerator.docker -t git.project-hobbit.eu:4567/papv/versioningdatagenerator .
	docker build -f docker/versioningtaskgenerator.docker -t git.project-hobbit.eu:4567/papv/versioningtaskgenerator .
	docker build -f docker/versioningevaluationmodule.docker -t git.project-hobbit.eu:4567/papv/versioningevaluationmodule .
	docker build -f docker/versioningvirtuososystemadapter.docker -t git.project-hobbit.eu:4567/papv/versioningsystem .

	docker push git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller
	docker push git.project-hobbit.eu:4567/papv/versioningdatagenerator
	docker push git.project-hobbit.eu:4567/papv/versioningtaskgenerator
	docker push git.project-hobbit.eu:4567/papv/versioningevaluationmodule
	docker push git.project-hobbit.eu:4567/papv/versioningsystem
