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
                    "dim": "5"
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
beeline -u "jdbc:hive2://127.0.0.1:${SQLREC_THRIFT_PORT}/default;auth=noSasl" -f ${dir}/init.sql.tmp

python3 -m venv ${dir}/.venv
source ${dir}/.venv/bin/activate
pip install -r ${dir}/requirements.txt
python ${dir}/mock_data.py



