default: build dockerize

build:	
	mvn clean package -U -Dmaven.test.skip=true

dockerize:
	docker build -f docker/versioningbenchmarkcontroller.docker -t git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller:2.0 .
	docker build -f docker/versioningdatagenerator.docker -t git.project-hobbit.eu:4567/papv/versioningdatagenerator:2.0 .
	docker build -f docker/versioningtaskgenerator.docker -t git.project-hobbit.eu:4567/papv/versioningtaskgenerator:2.0 .
	docker build -f docker/versioningevaluationmodule.docker -t git.project-hobbit.eu:4567/papv/versioningevaluationmodule:2.0 .
	docker build -f docker/versioningvirtuososystemadapter.docker -t git.project-hobbit.eu:4567/papv/versioningsystem:2.0 .
#       docker build -f docker/versioningr43plessystemadapter.docker -t git.project-hobbit.eu:4567/papv/versioningr43plessystem .

	docker push git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller:2.0
	docker push git.project-hobbit.eu:4567/papv/versioningdatagenerator:2.0
	docker push git.project-hobbit.eu:4567/papv/versioningtaskgenerator:2.0
	docker push git.project-hobbit.eu:4567/papv/versioningevaluationmodule:2.0
	docker push git.project-hobbit.eu:4567/papv/versioningsystem:2.0
#       docker push git.project-hobbit.eu:4567/papv/versioningr43plessystem

