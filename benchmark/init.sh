#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile
set -ex
dir=$(dirname $(realpath $0))

export BASE_DIR=$(dirname ${dir})/deploy
source ${dir}/../deploy/env.sh


export schema='{
        "autoId": false,
        "enabledDynamicField": false,
        "fields": [
            {
                "fieldName": "id",
                "dataType": "Int64",
                "isPrimary": true
            },
            {
                "fieldName": "embedding",
                "dataType": "FloatVector",
                "elementTypeParams": {
                    "dim": "8"
                }
            },
            {
                "fieldName": "name",
                "dataType": "VarChar",
                "elementTypeParams": {
                    "max_length": 512
                }
            }
        ]
    }'
export indexParams='[
        {
            "fieldName": "embedding",
            "metricType": "COSINE",
            "indexName": "embedding",
            "indexType": "AUTOINDEX"
        },
        {
            "fieldName": "id",
            "indexName": "id",
            "indexType": "AUTOINDEX"
        }
    ]'
export CLUSTER_ENDPOINT="http://${NODE_IP}:${MILVUS_PORT}"
export TOKEN="root:Milvus"
curl --request POST \
--url "${CLUSTER_ENDPOINT}/v2/vectordb/collections/create" \
--header "Authorization: Bearer ${TOKEN}" \
--header "Content-Type: application/json" \
-d "{
    \"collectionName\": \"item_embedding\",
    \"schema\": $schema,
    \"indexParams\": $indexParams
}"


envsubst < ${dir}/init.sql > ${dir}/init.sql.tmp
beeline -u "jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default;auth=noSasl" -f ${dir}/init.sql.tmp

python3 -m venv ${dir}/.venv
source ${dir}/.venv/bin/activate
pip install -r ${dir}/requirements.txt
python ${dir}/mock_data.py

PARQUET_FILE="${dir}/behavior_sample.parquet"
HDFS_WAREHOUSE_DIR="/user/hive/warehouse"
TABLE_NAME="behavior_sample"
PARTITION_DATE="dt=2024-01-01"
if [ -f "$PARQUET_FILE" ]; then
    echo "Uploading parquet file to HDFS..."
    hdfs dfs -mkdir -p ${HDFS_WAREHOUSE_DIR}/${TABLE_NAME}/${PARTITION_DATE}
    hdfs dfs -put -f ${PARQUET_FILE} ${HDFS_WAREHOUSE_DIR}/${TABLE_NAME}/${PARTITION_DATE}/
    echo "Parquet file uploaded successfully"
else
    echo "Warning: Parquet file not found: ${PARQUET_FILE}"
    exit 1
fi
hive -f ${dir}/init_hive.sql

# Check if wrk is installed, install it if not (Debian system)
if ! which wrk > /dev/null 2>&1; then
    echo "wrk not found, installing..."
    sudo apt-get update
    sudo apt-get install -y wrk
else
    echo "wrk is already installed"
fi
