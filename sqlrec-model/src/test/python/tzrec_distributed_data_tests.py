"""Verify native distributed Parquet sharding with spawned DataLoader workers."""

import json
import multiprocessing as mp
import os
import shutil
import tempfile
import unittest
from datetime import timedelta

import pyarrow as pa
from pyarrow import parquet
from torch import distributed as dist

from tzrec.constant import Mode
from tzrec.datasets.dataset import create_dataloader
from tzrec.features.feature import create_features
from tzrec.protos import data_pb2, feature_pb2
from tzrec.utils import misc_util


def _read_distributed_parquet(rank, port, test_dir, num_workers, mode, resume):
    os.environ.update(
        RANK=str(rank), WORLD_SIZE="2", MASTER_ADDR="127.0.0.1", MASTER_PORT=str(port)
    )
    mp.set_start_method("spawn", force=True)
    dist.init_process_group(backend="gloo", timeout=timedelta(seconds=60))
    try:
        data_config = data_pb2.DataConfig(
            batch_size=4,
            num_workers=num_workers,
            dataset_type=data_pb2.DatasetType.ParquetDataset,
            fg_mode=data_pb2.FgMode.FG_NONE,
            label_fields=["label"],
            pin_memory=False,
        )
        features = create_features(
            [
                feature_pb2.FeatureConfig(
                    raw_feature=feature_pb2.RawFeature(feature_name="value")
                )
            ]
        )
        dataloader = create_dataloader(
            data_config, features, os.path.join(test_dir, "data.parquet"), mode=mode
        )
        epochs = []
        for epoch in range(2):
            labels = []
            iterator = dataloader.get_iterator()
            if resume and epoch == 0:
                batch = next(iterator)
                labels.extend(batch.labels["label"].tolist())
                states = [None, None]
                dist.all_gather_object(states, batch.checkpoint_info)
                checkpoint_state = {}
                for state in states:
                    checkpoint_state.update(state)
                del iterator, dataloader
                dataloader = create_dataloader(
                    data_config,
                    features,
                    os.path.join(test_dir, "data.parquet"),
                    mode=mode,
                    checkpoint_state=checkpoint_state,
                )
                iterator = dataloader.get_iterator()
            for batch in iterator:
                labels.extend(batch.labels["label"].tolist())
            epochs.append(labels)
        del dataloader
        with open(os.path.join(test_dir, f"rank-{rank}.json"), "w") as output:
            json.dump(epochs, output)
    finally:
        dist.destroy_process_group()


class DistributedParquetDatasetTest(unittest.TestCase):
    def test_single_worker_per_rank(self):
        self._check_shards(0, Mode.TRAIN, False)

    def test_spawned_training_workers(self):
        self._check_shards(2, Mode.TRAIN, False)

    def test_spawned_evaluation_workers(self):
        self._check_shards(2, Mode.EVAL, False)

    def test_spawned_workers_resume_without_duplicate_rows(self):
        self._check_shards(2, Mode.TRAIN, True)

    def _check_shards(self, num_workers, mode, resume):
        test_dir = tempfile.mkdtemp(prefix="sqlrec-shards-")
        processes = []
        try:
            parquet.write_table(
                pa.table(
                    {
                        "value": [float(i) for i in range(128)],
                        "label": list(range(128)),
                    }
                ),
                os.path.join(test_dir, "data.parquet"),
            )
            port = misc_util.get_free_port()
            context = mp.get_context("spawn")
            for rank in range(2):
                process = context.Process(
                    target=_read_distributed_parquet,
                    args=(rank, port, test_dir, num_workers, mode, resume),
                )
                process.start()
                processes.append(process)
            for process in processes:
                process.join(timeout=90)
                self.assertEqual(process.exitcode, 0)
            ranks = []
            for rank in range(2):
                with open(os.path.join(test_dir, f"rank-{rank}.json")) as output:
                    ranks.append(json.load(output))
            for epoch in range(2):
                self.assertEqual(sorted(ranks[0][epoch]), list(range(64)))
                self.assertEqual(sorted(ranks[1][epoch]), list(range(64, 128)))
                self.assertEqual(
                    sorted(ranks[0][epoch] + ranks[1][epoch]), list(range(128))
                )
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join(timeout=10)
            shutil.rmtree(test_dir)


if __name__ == "__main__":
    unittest.main()
