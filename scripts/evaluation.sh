mvn clean package -U -Dmaven.test.skip=true
docker build -f docker/versioningevaluationmodule.docker -t git.project-hobbit.eu:4567/papv/versioningevaluationmodule:2.0 . 
docker push git.project-hobbit.eu:4567/papv/versioningevaluationmodule:2.0
