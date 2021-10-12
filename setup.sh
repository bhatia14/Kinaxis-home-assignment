#!/bin/bash

echo "Building application..."
mvn -DskipTests clean package

# Directory where spark-submit is defined
# Install spark from https://spark.apache.org/downloads.html
# NOTE: you need to change this to your spark installation
#SPARK_HOME=/Documents/tutorials/spark-3.1.2-bin-hadoop3.2
SPARK_HOME=
# JAR
JARFILE=`pwd`/target/Kinaxis-home-assignment-1.0-SNAPSHOT.jar

# Run it locally
${SPARK_HOME}/bin/spark-submit --class HomeAssignment --master local $JARFILE
