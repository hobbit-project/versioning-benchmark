
mvn clean package -U -Dmaven.test.skip=true
docker build -f docker/versioningtaskgenerator.docker -t git.project-hobbit.eu:4567/papv/versioningtaskgenerator:develop .
docker push git.project-hobbit.eu:4567/papv/versioningtaskgenerator:develop

