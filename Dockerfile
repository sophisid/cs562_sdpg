# Use an official OpenJDK runtime as a base image
FROM openjdk:8

# Install sbt (Scala Build Tool)
RUN apt-get update && apt-get install -y curl gnupg && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add - && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
    apt-get update && apt-get install -y sbt

# Set the working directory to /app
WORKDIR /app

# Copy the entire project from your context into the /app directory
COPY . /app

# Change directory to schemadiscovery inside the container
WORKDIR /app/schemadiscovery

# Expose any ports if necessary (this is optional and depends on your application)
EXPOSE 8080

# Define the command to run the sbt application
CMD ["sbt", "run"]
