#!/usr/bin/env bash

set -e

root="/mnt/c/Users/${USER}/Fun/programming/streaming-notes/flink/"

repoName="word_count"
moduleName=$(basename $(dirname $(dirname $(realpath ${BASH_SOURCE}))))
modelName=$(basename $(dirname $(realpath ${BASH_SOURCE})))

param=${@}
flinkRun="/mnt/c/flink-1.12.2/bin/flink run"
target="target/scala-2.11/${modelName}.jar"

logDir="${root}/logs/${repoName}/${moduleName}_${modelName}"
mkdir -p ${logDir}
#logPath="${logDir}/${param}.log"
logPath="${logDir}/.log"
logPath=${logPath// /.}

cd `dirname $0` # move to directory where this shell script is in
${flinkRun} \
    ${target} ${param} \
    |& tee ${logPath}

flinkRunExit=${PIPESTATUS[0]}
exit ${flinkRunExit}