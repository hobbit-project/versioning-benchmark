FROM maven:3.5-jdk-8

RUN git clone -b develop https://github.com/plt-tud/r43ples.git /r43ples

WORKDIR /r43ples

RUN mvn package -DskipTests

RUN mkdir -p /r43ples/database/dataset

ADD target/versioning-0.0.1-SNAPSHOT.jar /versioning.jar

COPY r43ples_system /r43ples

CMD ["/r43ples/run.sh"]
