default: build dockerize

build:	
	mvn clean package -U -Dmaven.test.skip=true

dockerize:
	docker build -f docker/versioningbenchmarkcontroller.docker -t git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller:develop .
	docker build -f docker/versioningdatagenerator.docker -t git.project-hobbit.eu:4567/papv/versioningdatagenerator:develop .
	docker build -f docker/versioningtaskgenerator.docker -t git.project-hobbit.eu:4567/papv/versioningtaskgenerator:develop .
	docker build -f docker/versioningevaluationmodule.docker -t git.project-hobbit.eu:4567/papv/versioningevaluationmodule:develop .
	docker build -f docker/versioningvirtuososystemadapter.docker -t git.project-hobbit.eu:4567/papv/versioningsystem:develop .

	docker push git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller:develop
	docker push git.project-hobbit.eu:4567/papv/versioningdatagenerator:develop
	docker push git.project-hobbit.eu:4567/papv/versioningtaskgenerator:develop
	docker push git.project-hobbit.eu:4567/papv/versioningevaluationmodule:develop
	docker push git.project-hobbit.eu:4567/papv/versioningsystem:develop

