# Hugging Face Transformers Model

**Model name**: `huggingface.transformers`

For this backend, `TRAIN MODEL` downloads a selected Hugging Face Hub revision and stores it through the Hadoop CLI as an origin checkpoint that can be served directly. `EXPORT MODEL` is not supported yet.

Supported tasks are `text-classification`, `text-generation`, `embedding`, and `image-embedding`. `/predict` generation accepts plain prompts. Text generation and text embedding also expose the OpenAI-compatible endpoints below by default. Image embedding accepts HTTP/HTTPS URLs only.

Text classification follows the checkpoint's `problem_type`: single-label models with multiple outputs use softmax, while multi-label and single-output models use sigmoid. All return the highest-scoring label/score. Checkpoints explicitly declaring `problem_type=regression` are rejected at service load; single-output models without a declared type retain sigmoid compatibility. Input column options are case-insensitive and runtime configuration uses the declared SQL field names. Classification, text embedding and generation require non-null STRING text inputs, including configured paired text. Nulls, missing fields and non-string values return HTTP 400; empty strings remain valid inputs.

## Hugging Face Task Options

`task` determines the SQL output fields and cannot be changed through training or service overrides; create a new model to switch tasks. Serving also checks that the checkpoint task matches the declared service task at startup.

`task` and `repo_id` are required. Declare the input column as `STRING` in the Model and select the column option for the task:

| task | Required input option | Output fields |
| --- | --- | --- |
| `text-classification` | `text_column` | `label` (STRING), `score` (FLOAT) |
| `text-generation` | `prompt_column` | `generated_text` (STRING) |
| `embedding` | `text_column` | `embedding` (ARRAY&lt;FLOAT&gt;) |
| `image-embedding` | `image_column` | `embedding` (ARRAY&lt;FLOAT&gt;) |

`repo_id` identifies the Hub repository. `revision` selects a branch, tag, or commit and defaults to `main`; pin a commit for reproducible downloads. This example defines a text embedding model:

```sql
CREATE MODEL text_embedding_model (
    text STRING
) WITH (
    'model' = 'huggingface.transformers',
    'task' = 'embedding',
    'repo_id' = 'intfloat/multilingual-e5-small',
    'text_column' = 'text',
    'pooling' = 'mean',
    'normalize' = 'true'
);

TRAIN MODEL text_embedding_model CHECKPOINT = 'v1' WITH (
    'revision' = 'main'
);

CREATE SERVICE `text-embedding-service`
    ON MODEL text_embedding_model
    CHECKPOINT = 'v1'
    WITH (
        'device' = 'auto',
        'inference_batch_size' = '32'
    );
```

Private repositories can reference a Kubernetes Secret with `hf_token_secret` and `hf_token_secret_key` in the TRAIN parameters. Serving reads only from the checkpoint and does not contact the Hub.

## Hugging Face Service Options

Ordinary inference parameters use this precedence: explicit `CREATE SERVICE ... WITH` values, parameters explicitly declared in the Model, training parameters saved in the checkpoint, then built-in defaults. For example, if the Model declares `pooling=mean` and TRAIN overrides it to `cls`, a service without an explicit override still uses `mean`; set `pooling=cls` in SERVICE WITH to use `cls`. Parameters set only in TRAIN and absent from both the Model and SERVICE are inherited from the checkpoint. The `task` must match the Model declaration and checkpoint, and `trust_remote_code` uses the value recorded in the checkpoint.

Set these as needed in `CREATE SERVICE ... WITH (...)`:

| Parameter | Default | Usage |
| --- | --- | --- |
| `device` | `auto` | `auto`, `cpu`, or `cuda` |
| `inference_batch_size` | `8` | Internal inference batch size |
| `dtype` | `auto` | `auto`, `float32`, `float16`, or `bfloat16` |
| `pod_gpu` | `0` | GPU count; request a positive number and provide available GPU nodes for GPU inference |
| `pod_gpu_resource` | `nvidia.com/gpu` | GPU resource name in the cluster |

For GPU inference, set `'device' = 'cuda', 'pod_gpu' = '1'`. The cluster also needs compatible GPU drivers, a device plugin, and a runtime.

## OpenAI-Compatible Endpoints

Endpoints are always available, with no enable/disable option. They share the existing service, port and loaded model; the stored URL and `call_service` keep using `/predict`.

| Endpoint | task | SQL UDF |
| --- | --- | --- |
| `GET /v1/models` | Text generation or embedding | Lists the single alias; other tasks return an empty list |
| `POST /v1/chat/completions` | `text-generation` | `llm_complete` |
| `POST /v1/embeddings` | `embedding` | `llm_embed_text` |

`model` must exactly match `served_model_name`, which defaults to the SQL Model name. Requests cannot switch models. Classification and images do not implement these inference protocols or Ark embeddings.

```sql
SELECT llm_embed_text(
    'http://text-embedding-service.<namespace>.svc.cluster.local:80/v1/embeddings',
    '', 'text_embedding_model', title, '{}'
) AS embedding FROM items;

-- Assumes chat_model has an origin checkpoint and its own chat template.
CREATE SERVICE `chat-service` ON MODEL chat_model CHECKPOINT = 'v1'
WITH ('served_model_name' = 'article_summary', 'max_new_tokens' = '256');

SELECT llm_complete(
    'http://chat-service.<namespace>.svc.cluster.local:80/v1/chat/completions',
    '', 'article_summary', content,
    '{"request":{"temperature":0,"max_completion_tokens":128},"runtime":{"timeout_ms":120000}}'
) AS summary FROM articles;
```

Replace `<namespace>` and ensure SQL/Flink workers can reach the service. 

Chat supports exactly one user string message, `stream=false`, `n=1`, `store=false`, `temperature`, `top_p`, `max_completion_tokens` and optional `user`. It uses the tokenizer's chat template; missing templates return 400 `chat_template_missing` while `/predict` remains usable. Multi-turn/system/developer messages, tools, `reasoning_effort`, `stop`, `max_tokens`, structured outputs and unknown fields are rejected.

`max_completion_tokens` maps to `max_new_tokens`; the service setting (default 128) is both the default and hard cap. `temperature=0` uses greedy decoding, positive temperature enables sampling, and explicit `top_p<1` also enables sampling when temperature is omitted. Ranges are `[0,2]` and `(0,1]`; combining `temperature=0` with `top_p<1` is rejected. Omitted parameters retain service defaults; overrides are request-local.

Embeddings accept a nonblank string or array of nonblank strings, with `encoding_format=float` only. Existing pooling, normalization and native dimensions are preserved. Optional `dimensions` must equal the native dimension. Callers supply model-specific prefixes such as `query:`. New endpoints reject input beyond `max_length`; chat also checks template tokens plus output budget against the model context. Usage counts actual token IDs.

| Option | Default | Description |
| --- | --- | --- |
| `served_model_name` | SQL Model name | Single public alias |
| `openai_api_key_secret` | Unset | Optional K8s Secret injected as `SQLREC_OPENAI_API_KEY` |
| `openai_api_key_secret_key` | `token` | Secret key |
| `openai_max_request_bytes` | `1048576` | `/v1/*` body limit, bytes |
| `openai_max_batch_inputs` | `32` | Texts per embedding request; internally chunked by `inference_batch_size` |

These options come from Model/SERVICE, not TRAIN checkpoint parameters. When a Secret is configured, `/v1/*` requires the matching Bearer token; use a context-injected key such as `` `get`('llm_api_key') `` as the second UDF argument. Otherwise pass an empty key. Authentication does not cover `/predict`; keep the service internal and expose only required routes at an external ingress.

One inference request runs per Pod across `/predict` and `/v1`; busy requests return 429. Scale replicas to match SQL concurrency. UDFs call synchronously per row without retries, so rate limiting fails SQL calls. Client timeout may not immediately stop GPU work. The new error envelope contains `error`: invalid input 400, key 401, unknown model 404, body limit 413, busy 429, unloaded model 503, internal failure 500.

::: details Task-Specific Options
Text embeddings support `pooling` (default `mean`) and `normalize` (default `true`). Text classification can use `text_pair_column` for a second text column. Text generation supports `max_new_tokens` (default `128`), `do_sample` (default `false`), `temperature`, and `top_p`.

For image embeddings, `image_column` must contain HTTP/HTTPS URLs. Limit downloads with `image_url_allowed_hosts`, `image_download_timeout_ms` (default `5000`), `image_max_bytes` (default 10 MiB), and `image_max_pixels` (default 20 million).

`image_url_allowed_hosts` restricts downloads to exact hostnames, including redirects. When unset, any HTTP/HTTPS host is allowed. Configure network access and the download limits for your data sources.
:::
