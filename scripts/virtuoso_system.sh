mvn clean package -U -Dmaven.test.skip=true
docker build -f docker/versioningvirtuososystemadapter.docker -t git.project-hobbit.eu:4567/papv/versioningsystem:develop .
docker push git.project-hobbit.eu:4567/papv/versioningsystem:develop
