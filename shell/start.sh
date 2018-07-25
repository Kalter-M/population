#!/usr/bin/env bash
while [ -n "$1" ]
do
case "$1" in
--job) job="$2"
shift ;;
--load_from) load_from="$2"
shift ;;
--save_to) save_to="$2"
shift ;;
--period_start) period_start="$2"
shift ;;
--period_end) period_end="$2"
shift ;;
*) echo "$1 is not a valid option";;
esac
shift
done
echo "Start build project..."
(cd ../ && sbt assembly)
echo "Build done."
echo "MongoDB startup..."
docker pull mongo:3.6
docker run -d -p 27017:27017 mongo:3.6
echo "MongoDB startup done."
echo "Running job..."
export PROJECT_PATH=${PWD}/..
# Env var SPARK_HOME should be set, like
# export SPARK_HOME=/usr/local/spark
export unsd_fm=$PROJECT_PATH/target/scala-2.11/classes/sources/unsd-citypopulation-year-fm.csv
export unsd_both=$PROJECT_PATH/target/scala-2.11/classes/sources/unsd-citypopulation-year-both.csv
cd $SPARK_HOME
./bin/spark-submit \
--class ru.dsr.bigdata.Launcher \
${PROJECT_PATH}/target/scala-2.11/population-assembly-1.0.0.jar \
job=$job \
load_from=$load_from \
save_to=$save_to \
period_start=$period_start \
period_end=$period_end
echo "Job done."
