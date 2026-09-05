#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile
set -ex
dir=$(dirname $(realpath $0))

export BASE_DIR=$(dirname $(dirname ${dir}))/deploy
source ${BASE_DIR}/env.sh
bash ${BASE_DIR}/kyuubi/deploy.sh


if command -v wrk >/dev/null 2>&1; then
    echo "wrk is already installed"
else
    echo "wrk not found, installing..."
    case "${DEPLOY_OS}" in
        darwin)
            if ! command -v brew >/dev/null 2>&1; then
                echo "ERROR: Homebrew is required to install wrk on macOS." >&2
                exit 1
            fi
            brew install wrk
            ;;
        linux)
            if ! command -v apt-get >/dev/null 2>&1; then
                echo "ERROR: automatic wrk installation currently requires apt-get on Linux." >&2
                exit 1
            fi
            sudo apt-get update
            sudo apt-get install -y wrk
            ;;
        *)
            echo "ERROR: unsupported operating system: ${DEPLOY_OS}" >&2
            exit 1
            ;;
    esac

    if ! command -v wrk >/dev/null 2>&1; then
        echo "ERROR: wrk is still unavailable after installation." >&2
        exit 1
    fi
fi


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

python3 -m venv ${dir}/.venv
source ${dir}/.venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r ${dir}/requirements.txt
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

envsubst < ${dir}/init_flink_table.sql > ${dir}/init_flink_table.sql.tmp
beeline -u "jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default;auth=noSasl" -f ${dir}/init_flink_table.sql.tmp
hive -f ${dir}/init_hive_table.sql

echo "Computing features from MovieLens data..."
beeline -u "jdbc:hive2://${NODE_IP}:${KYUUBI_PORT}/default" -f ${dir}/compute_features.sql
echo "Feature computation completed"

echo "Train model..."
beeline -u "jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default;auth=noSasl" -f ${dir}/init_model.sql
echo "Model trained successfully"

echo "Loading features to Redis..."
beeline -u "jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default;auth=noSasl" -f ${dir}/load_features.sql
echo "Features loaded successfully"

beeline -u "jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default;auth=noSasl" -f ${dir}/init_sqlrec_sql.sql

echo "Test rec..."
beeline -u "jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default;auth=noSasl" -e "
cache table t1 as select cast(1 as bigint) as user_id;
call main_rec(t1);
set rank_fun=rank_fun;
set use_recall_service=true;
call main_rec(t1);
"
