"""Exercise two torchrun node agents with spawned workers and checkpoint resume."""

import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq
from tzrec.utils import config_util
from tzrec.utils.checkpoint_util import EPOCHS_COMPLETED, latest_checkpoint

from tzrec_pipeline_smoke import config
from tzrec_summaries_integration import write_data


def train_nodes(root, pipeline_path, name, resume=False, fail_after_checkpoint=False):
    """Start both node agents and stop the whole group if either agent fails."""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = {**os.environ, "USE_SPAWN_MULTI_PROCESS": "1", "USE_FARM_HASH_TO_BUCKETIZE": "true",
           "TORCH_MANUAL_SEED": "123", "NUMPY_MANUAL_SEED": "123"}
    agents, logs = [], []
    try:
        for node_rank in range(2):
            log = (root / f"{name}-node-{node_rank}.log").open("w")
            logs.append(log)
            entrypoint = str(Path(__file__).resolve()) if fail_after_checkpoint else "/app/run.py"
            command = ["torchrun", "--nnodes=2", "--nproc-per-node=1", f"--node_rank={node_rank}",
                       "--master_addr=127.0.0.1", f"--master_port={port}", entrypoint,
                       "--mode", "train", "--pipeline_config_path", str(pipeline_path)]
            if resume:
                command.append("--continue_train")
            if fail_after_checkpoint:
                command.append("--fail_after_checkpoint")
            agents.append(subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                           start_new_session=True))
        deadline = time.monotonic() + 180
        while any(agent.poll() is None for agent in agents):
            if any(agent.poll() not in (None, 0) for agent in agents):
                if fail_after_checkpoint:
                    break
                raise AssertionError(f"Node agent failed: {name}; logs in {root}")
            if time.monotonic() >= deadline:
                raise AssertionError(f"Distributed training timed out: {name}; logs in {root}")
            time.sleep(.1)
        if fail_after_checkpoint:
            for log in logs:
                log.flush()
            assert any(agent.poll() not in (None, 0) for agent in agents), name
            assert any("injected checkpoint failure" in path.read_text()
                       for path in root.glob(f"{name}-node-*.log")), name
        else:
            assert all(agent.returncode == 0 for agent in agents), name
    except Exception as error:
        for log in logs:
            log.flush()
        details = "\n".join(path.read_text()[-8000:] for path in root.glob(f"{name}-node-*.log"))
        raise AssertionError(f"{name}: {error}\n{details}") from error
    finally:
        for agent in agents:
            if agent.poll() is None:
                os.killpg(agent.pid, signal.SIGTERM)
        for agent in agents:
            try:
                agent.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(agent.pid, signal.SIGKILL)
                agent.wait(timeout=15)
        for log in logs:
            log.close()


def main():
    with tempfile.TemporaryDirectory(prefix="sqlrec-distributed-") as temporary:
        root = Path(temporary)
        write_data(root)
        positives = pq.read_table(root / "dssm_data.parquet")
        pq.write_table(positives.append_column("item_id", pa.array(range(128))), root / "dssm_data.parquet")
        pq.write_table(positives.slice(0, 16).append_column("item_id", pa.array(range(16))), root / "dssm_eval.parquet")
        for architecture in ("wide_and_deep", "dssm"):
            pipeline = config(root, architecture)
            pipeline.data_config.num_workers = 2
            pipeline.train_config.num_epochs = 1
            pipeline.train_config.save_checkpoints_steps = 0
            pipeline.train_config.save_checkpoints_epochs = 1
            if architecture == "dssm":
                pipeline.train_input_path = str(root / "dssm_data.parquet")
                pipeline.eval_input_path = str(root / "dssm_eval.parquet")
                pipeline.model_config.dssm.in_batch_negative = False
                feature = pipeline.feature_configs.add().id_feature
                feature.feature_name, feature.expression = "item_id", "item:item_id"
                feature.num_buckets, feature.embedding_dim = 256, 8
                item_group = pipeline.model_config.feature_groups[1]
                item_group.feature_names.append("item_id")
                sampler = pipeline.data_config.pool_negative_sampler
                sampler.input_path = pipeline.train_input_path
                sampler.num_sample, sampler.pool_size, sampler.item_id_field = 8, 64, "item_id"
                sampler.attr_fields.extend(item_group.feature_names)
            path = root / f"{architecture}.config"
            config_util.save_message(pipeline, str(path))
            train_nodes(root, path, architecture)
            checkpoint, step = latest_checkpoint(pipeline.model_dir)
            state = json.loads((Path(checkpoint) / "dataloader_state.json").read_text())
            assert state[EPOCHS_COMPLETED] == 1, state
            assert step == 7, (architecture, step)

            pipeline.train_config.num_epochs = 2
            pipeline.train_config.save_checkpoints_epochs = 0
            pipeline.train_config.save_checkpoints_steps = 4
            # Once this job has a checkpoint, its original fine-tune source is unnecessary.
            pipeline.train_config.fine_tune_checkpoint = str(root / "missing-base")
            config_util.save_message(pipeline, str(path))
            train_nodes(root, path, architecture + "-failure", resume=True, fail_after_checkpoint=True)
            checkpoint, failed_step = latest_checkpoint(pipeline.model_dir)
            assert failed_step == 8, (architecture, failed_step)
            train_nodes(root, path, architecture + "-resume", resume=True)
            checkpoint, resumed_step = latest_checkpoint(pipeline.model_dir)
            state = json.loads((Path(checkpoint) / "dataloader_state.json").read_text())
            assert state[EPOCHS_COMPLETED] == 2, state
            assert resumed_step > failed_step, (architecture, resumed_step)
            assert list((Path(checkpoint) / "model").glob("__1_*.distcp")), checkpoint
            print(json.dumps({"architecture": architecture, "nodes": 2, "workers_per_rank": 2,
                              "initial_step": step, "failed_step": failed_step,
                              "resumed_step": resumed_step}), flush=True)


def failing_train():
    """Fail one rank after a complete distributed checkpoint has been written."""
    import torch.distributed as dist
    from run import main as train_main
    from tzrec.utils.checkpoint_util import CheckpointManager

    save_checkpoint = CheckpointManager.save

    def save_then_fail(self, *args, **kwargs):
        checkpoint = save_checkpoint(self, *args, **kwargs)
        dist.barrier()
        if dist.get_rank() == 1:
            raise RuntimeError("injected checkpoint failure")
        return checkpoint

    CheckpointManager.save = save_then_fail
    sys.argv.remove("--fail_after_checkpoint")
    train_main()


if __name__ == "__main__":
    if "--fail_after_checkpoint" in sys.argv:
        failing_train()
    else:
        main()
