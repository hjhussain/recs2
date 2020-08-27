#!/usr/bin/env bash

JAR=target/scala-2.11/LastFmAnalytics-assembly-0.1.jar

MAIN_CLASS="com.company.test.lastfm.LastFmJob"
INPUT_PATH=$1
OUTPUT_PATH=$2

if [[ "$INPUT_PATH" == "" ]]; then
    INPUT_PATH="src/test/resources/data/*"
fi

if [[ "$OUTPUT_PATH" == "" ]]; then
    OUTPUT_PATH="/tmp/lastfm_$(date +%s)"
fi

declare -A ARGS
ARGS[inputPath]=${INPUT_PATH}
ARGS[outputPath]=${OUTPUT_PATH}


ARGS_STR=""
for k in "${!ARGS[@]}"; do
  ARGS_STR="$ARGS_STR $k=${ARGS[$k]}"
done

spark-submit --class ${MAIN_CLASS} \
  --master "local[*]" \
  --deploy-mode client \
  --executor-memory 2g \
  --driver-memory 1g \
  --num-executors 2 \
  --executor-cores 4 \
  --conf spark.yarn.maxAppAttempts=1 \
  $JAR $ARGS_STR
