#!/usr/bin/env bash

usage() {
    echo
    echo "usage: $0 OPTIONS"
    echo "OPTIONS:"
    echo "   -d: date"
    echo "   -k: instrumentation key"
    echo "   -i: input blob"
    echo "   -o: output blob"
    echo "   -h: display help"
}

while getopts ":d:k:i:o:h" opt; do
        case "${opt}" in
                d) DATE_PARTITION=${OPTARG} ;;
                k) MONITOR_INSTRUMENT_KEY=${OPTARG} ;;
                i) INPUT_BLOB=${OPTARG} ;;
                o) OUTPUT_BLOB=${OPTARG} ;;
                h) usage; exit ;;
                *) echo "Unknown option."; usage; exit 1; ;;
        esac
done
shift $((OPTIND-1))

FROM_DATE=$(date --date '-1 day' +"%Y/%m/%d")

if [ "$DATE_PARTITION" != "" ]; then
    FROM_DATE=$(date --date="$DATE_PARTITION" +"%Y/%m/%d")
fi

MONITOR_Q_STORE="asosdsmonitoringprod"

FS_MNT=/filestore

if [[ ${INPUT_BLOB} == "" || ${OUTPUT_BLOB} == "" ]]; then
    echo "need to pass input and output blob paths"
    exit 1
fi

docker_run() {
    sudo docker run -d -h recs --rm --name recsinmysize --mount source=filestore,target=$FS_MNT \
     -e SERVICE_APP_CLIENT_ID=${SERVICE_APP_CLIENT_ID} \
     -e SERVICE_APP_CLIENT_SECRET=${SERVICE_APP_CLIENT_SECRET} \
     -e SERVICE_APP_TENANT=${SERVICE_APP_TENANT} \
     bigdataengdev.azurecr.io/asos/recsinmysize:version1 \
     python3 application.py \
      -i ${INPUT_BLOB} \
      -o ${OUTPUT_BLOB} \
      -d 30 \
      -t "${FROM_DATE}" \
      -c ${MONITOR_Q_STORE} \
      -k ${MONITOR_INSTRUMENT_KEY} \
      -a $FS_MNT/input \
      -b $FS_MNT/output \
      -g $FS_MNT/logs/recsinmysize_$(date +"%Y_%m_%d").log \
      -m $FS_MNT/model/mdoel.pickle
}

docker_run
