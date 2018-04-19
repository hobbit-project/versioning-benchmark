mvn clean package -U -Dmaven.test.skip=true
docker build -f docker/versioningevaluationmodule.docker -t git.project-hobbit.eu:4567/papv/versioningevaluationmodule:develop . 
docker push git.project-hobbit.eu:4567/papv/versioningevaluationmodule:develop
