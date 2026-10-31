# Hugging Face integration benchmark

Exercise SQLRec's `huggingface.transformers` backend with very small public
models on CPU. The benchmark uses public SQL to create models, download pinned
Hub snapshots with `TRAIN MODEL` (without `ON`), deploy origin checkpoints, and
register SQL functions/APIs that call the model service.

These are random testing checkpoints. A completed run proves the tested output
contracts and resource lifecycle; it does not measure sentiment accuracy,
language quality, or embedding retrieval quality.

## Models

| Task | Hub model | safetensors weights | Cases |
| --- | --- | --- | --- |
| `text-classification` | [tiny-random-bert](https://huggingface.co/hf-internal-testing/tiny-random-bert/tree/f171d7baecaf37b5da5a3616d8833b9969753535) | 520 KB | Single text and paired text |
| `text-generation` | [tiny-random-gpt2](https://huggingface.co/hf-internal-testing/tiny-random-gpt2/tree/71034c5d8bde858ff824298bdedc65515b97d2b9) | 454 KB | Greedy generation, at most 8 new tokens |
| `embedding` | [tiny-random-bert](https://huggingface.co/hf-internal-testing/tiny-random-bert/tree/f171d7baecaf37b5da5a3616d8833b9969753535) | 520 KB | Mean pooling, 32 dimensions, L2 normalization |
| `image-embedding` | [tiny-random-vit](https://huggingface.co/hf-internal-testing/tiny-random-vit/tree/96e253fe90cfcededfa8edf8b7e6f230f87a63a1) | 176 KB | CLS pooling, 32 dimensions, L2 normalization |

Weights total about 1.15 MB across the three distinct checkpoints. Each case
downloads its own snapshot; all five cases download about 2.2 MB of weights.
Configuration, tokenizer files, test images and the runtime container are extra.
Only the necessary safetensors and processor files are downloaded, excluding
TensorFlow, pickle, ONNX and duplicate weight formats. Model commit revisions
and versioned public test image URLs are fixed in [`config.py`](config.py).
No Hub token or `trust_remote_code` is required.

The image case uses OpenCV 4.10.0's `lena.jpg` (about 92 KB) and `fruits.jpg`
(about 82 KB), served directly from `raw.githubusercontent.com`. These small
fixtures avoid the Hugging Face dataset CDN redirect path. The three input
rows use the first image, the second image, and the first image again to check
consistency across internal batch boundaries.

## Run

Python 3.11+, a deployed SQLRec server with the Hugging Face backend, shared
Hadoop/JuiceFS storage, and Kubernetes access are required. The configured
`sqlrec/transformers:${SQLREC_VERSION}` runtime image must be available for the
cluster architecture. Download Pods need access to Hugging Face; image inference
Pods also need public HTTPS image access. No local PyTorch/Transformers install,
GPU, Kyuubi, Milvus, or training dataset is needed for this benchmark.

Run from the repository root:

```bash
bash bin/setup_python_env.sh
.venv/bin/python -m pip install -r benchmark/huggingface/requirements.txt
source deploy/env.sh

# All four task types, including two classification cases.
.venv/bin/python -m benchmark.huggingface --task all

# One task type only.
.venv/bin/python -m benchmark.huggingface --task image-embedding

# Change the number of timed three-row requests (default: 3).
.venv/bin/python -m benchmark.huggingface --task embedding --repeats 5

# Local contract tests; no model downloads or cluster connection.
.venv/bin/python -m unittest discover -s benchmark/huggingface/tests -v
```

`bin/test.sh -all` also runs this benchmark after deployment, Maven tests and
the SQL performance benchmark, before the model quality benchmark. Its existing
Python dependency installation covers the shared SQL clients and lifecycle
helpers used here.

The runner uses one CPU and 2 GiB for each download Job and service by default,
and runs cases sequentially, releasing the previous service before proceeding.
`BENCH_TRAIN_CPU`, `BENCH_TRAIN_MEMORY`, `BENCH_SERVICE_CPU`, and
`BENCH_SERVICE_MEMORY` override resources. `BENCH_TRANSFORMERS_VERSION` overrides
the runtime image tag, which defaults to `SQLREC_VERSION` without `-cpu`.
Authentication follows `AUTH_ENABLED`, `AUTH_USERNAME` and `AUTH_PASSWORD`.

## Proxies and private image URLs

The image loader delegates DNS resolution and routing to the HTTP client and
network environment. It does not perform a separate public-IP check, so
Fake-IP proxy mappings and private image URLs work without special options.
The optional `image_url_allowed_hosts` setting restricts exact source and
redirect hosts when configured. HTTP/HTTPS scheme, download size, pixel count,
timeout and redirect limits remain enforced.

Rebuild the Transformers runtime image on the cluster's Docker engine after
updating the Python implementation, before rerunning the image case:

```bash
bash bin/build_model_docker.sh transformers
.venv/bin/python -m benchmark.huggingface --task image-embedding
```

## Checks and reports

- A download must finish with a publicly reported `succeeded` origin checkpoint.
- Every case sends three rows through the SQL API with internal batch size 2,
  exercising padding, a partial final batch, and duplicate inputs with distinct IDs.
- Check output columns, exact row identities, valid classification labels and
  finite probabilities, STRING completions, and finite normalized 32-D vectors.
- Compare batched, single-row, reversed and repeated requests by row ID. Greedy
  text generation must be deterministic; an immediate EOS may produce an empty completion.
- Reject missing/null inputs, including the second text in paired classification.
  Image cases also reject local file URLs and FTP URLs. A valid request
  after these failures must still match the baseline.
- Cleanup uses the existing model benchmark lifecycle helper: drop each owned
  service and verify that its Kubernetes resources disappear and Jobs are quiet.
  Failure or Ctrl-C attempts cleanup and stops the run; unproven cleanup prevents
  the next case from starting. Submitted download mutations are not retried.

`runs/<timestamp>_<id>/summary.md` is the main report. `results.json` records
status, stage timings, timed batch durations, throughput and rejected-input
counts. Each case saves `config.json`, `statements.sql`, `resources.json`, and
three baseline predictions in `predictions.json`. Latency includes public API
transport and image downloads; download/deployment and correctness checks are
outside the timed requests. These tiny workloads do not represent production
throughput. Generated run reports are ignored by Git.

As in `model_quality`, models, checkpoints, completed Jobs, functions and APIs
remain for inspection. Only serving resources are released. Each run uses
unique names; it does not overwrite earlier runs or resume interrupted downloads.
