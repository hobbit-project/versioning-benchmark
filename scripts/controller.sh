mvn clean package -U -Dmaven.test.skip=true
docker build -f docker/versioningbenchmarkcontroller.docker -t git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller:develop .
docker push git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller:develop

