
mvn clean package -U -Dmaven.test.skip=true
docker build -f docker/versioningtaskgenerator.docker -t git.project-hobbit.eu:4567/papv/versioningtaskgenerator:2.0 .
docker push git.project-hobbit.eu:4567/papv/versioningtaskgenerator:2.0
