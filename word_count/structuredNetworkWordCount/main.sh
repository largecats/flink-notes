#!/usr/bin/env bash

set -e

if [[ ${USER} = "seatalk" ]]
then
    root="/home/seatalk"
else
    root="/home/datadev/${USER}"
fi

repoName="word_count"
moduleName=$(basename $(dirname $(dirname $(realpath ${BASH_SOURCE}))))
modelName=$(basename $(dirname $(realpath ${BASH_SOURCE})))

param=${@}
sparkSubmit="/usr/local/spark2.4.3/bin/spark-submit"
deployMode="client"
sparkConfig=$(cat <<-END
  --conf spark.yarn.maxAppAttempts=1
END
)
target="target/scala-2.11/${modelName}.jar"
sparkAppName="${moduleName}_${modelName} ${param}"

logDir="${root}/logs/${repoName}/${moduleName}_${modelName}"
mkdir -p ${logDir}
logPath="${logDir}/${param}.log"
logPath=${logPath// /.}

cd `dirname $0` # move to directory where this shell script is in
${sparkSubmit} \
    --name "${sparkAppName}" \
    --deploy-mode ${deployMode} \
    ${sparkConfig} \
    ${target} ${param} \
    |& tee -a ${logPath}

sparkSubmitExit=${PIPESTATUS[0]}
exit ${sparkSubmitExit}