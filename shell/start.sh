#!/usr/bin/env bash
echo "Start build project..."
(cd ../ && sbt assembly)
echo "Build done."
echo "MongoDB startup..."
docker run -d -p 27017:27017 mongo:3.6
echo "MongoDB startup done."
echo "Running job..."
PROJECT_PATH=${PWD}/..
export SPARK_HOME=/usr/local/spark
export unsd_fm=$PROJECT_PATH/target/scala-2.11/classes/sources/unsd-citypopulation-year-fm.csv
export unsd_both=$PROJECT_PATH/target/scala-2.11/classes/sources/unsd-citypopulation-year-both.csv
cd $SPARK_HOME
./bin/spark-submit \
--class ru.dsr.bigdata.Launcher \
${PROJECT_PATH}/target/scala-2.11/population-assembly-1.0.0.jar
echo "Job done."
