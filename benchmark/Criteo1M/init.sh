#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile
set -ex
dir=$(dirname $(realpath $0))

export BASE_DIR=${dir}/../../deploy
source ${BASE_DIR}/env.sh

# ---------------------------------------------------------------------------
# Step 1: Download and preprocess Criteo 1M dataset
# ---------------------------------------------------------------------------
echo "=== Step 1: Download and preprocess Criteo 1M ==="

python3 -m venv ${dir}/.venv
source ${dir}/.venv/bin/activate
pip install -r ${dir}/requirements.txt
python ${dir}/download_data.py

PARQUET="${dir}/criteo.parquet"

# ---------------------------------------------------------------------------
# Step 2: Upload parquet file to HDFS
# ---------------------------------------------------------------------------
echo "=== Step 2: Upload to HDFS ==="

HDFS_WAREHOUSE_DIR="/user/hive/warehouse"
PARTITION_DATE="dt=2024-01-01"

hdfs dfs -mkdir -p ${HDFS_WAREHOUSE_DIR}/criteo/${PARTITION_DATE}
hdfs dfs -put -f ${PARQUET} ${HDFS_WAREHOUSE_DIR}/criteo/${PARTITION_DATE}/
echo "Parquet uploaded"

# ---------------------------------------------------------------------------
# Step 3: Create Hive table
# ---------------------------------------------------------------------------
echo "=== Step 3: Create Hive table ==="

hive -f ${dir}/init_hive_table.sql

# ---------------------------------------------------------------------------
# Step 4: Train, export, serve, and test GBDT models
# ---------------------------------------------------------------------------
echo "=== Step 4: GBDT model training, export, serving, and inference test ==="

beeline -u "jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default;auth=noSasl" -f ${dir}/init_model.sql

echo "=== Criteo 1M GBDT benchmark completed! ==="
