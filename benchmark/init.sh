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
                "fieldName": "title",
                "dataType": "VarChar",
                "elementTypeParams": {
                    "max_length": 512
                }
            },
            {
                "fieldName": "genres",
                "dataType": "Array",
                "elementDataType": "VarChar",
                "elementTypeParams": {
                    "max_capacity": 64,
                    "max_length": 256
                }
            },
            {
                "fieldName": "embedding",
                "dataType": "FloatVector",
                "elementTypeParams": {
                    "dim": "64"
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
python ${dir}/download_data.py

HDFS_WAREHOUSE_DIR="/user/hive/warehouse"
PARTITION_DATE="dt=2024-01-01"

USERS_PARQUET="${dir}/ml_users.parquet"
MOVIES_PARQUET="${dir}/ml_movies.parquet"
RATINGS_PARQUET="${dir}/ml_ratings.parquet"

hdfs dfs -mkdir -p ${HDFS_WAREHOUSE_DIR}/ml_users/${PARTITION_DATE}
hdfs dfs -put -f ${USERS_PARQUET} ${HDFS_WAREHOUSE_DIR}/ml_users/${PARTITION_DATE}/
echo "Users parquet file uploaded successfully"

hdfs dfs -mkdir -p ${HDFS_WAREHOUSE_DIR}/ml_movies/${PARTITION_DATE}
hdfs dfs -put -f ${MOVIES_PARQUET} ${HDFS_WAREHOUSE_DIR}/ml_movies/${PARTITION_DATE}/
echo "Movies parquet file uploaded successfully"

hdfs dfs -mkdir -p ${HDFS_WAREHOUSE_DIR}/ml_ratings/${PARTITION_DATE}
hdfs dfs -put -f ${RATINGS_PARQUET} ${HDFS_WAREHOUSE_DIR}/ml_ratings/${PARTITION_DATE}/
echo "Ratings parquet file uploaded successfully"

hive -f ${dir}/init_hive.sql

echo "Computing features from MovieLens data..."
beeline -u "jdbc:hive2://${NODE_IP}:${KYUUBI_PORT}/default" -f ${dir}/compute_features.sql
echo "Feature computation completed"

echo "Loading features to Redis..."
beeline -u "jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default;auth=noSasl" -f ${dir}/load_features.sql
echo "Features loaded successfully"

if ! which wrk > /dev/null 2>&1; then
    echo "wrk not found, installing..."
    sudo apt-get update
    sudo apt-get install -y wrk
else
    echo "wrk is already installed"
fi
