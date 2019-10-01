#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_redistimeseries)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_redistimeseries not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi
FORMAT="redistimeseries"
# Load parameters - common

DATA_FILE_NAME=${DATA_FILE_NAME:-${FORMAT}-data.gz}
DATABASE_PORT=${DATABASE_PORT:-6379}
CONNECTIONS=${CONNECTIONS:-10}
PIPELINE=${PIPELINE:-100}


EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh


# Remove previous database

# Load new data
cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} \
                                --host=${DATABASE_HOST}:${DATABASE_PORT} \
                                --connections=${CONNECTIONS} --pipeline=${PIPELINE}
