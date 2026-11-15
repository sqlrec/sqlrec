# Hugging Face Transformers 模型

**模型名称**：`huggingface.transformers`

该模型类型把 `TRAIN MODEL` 定义为从 Hugging Face Hub 下载指定 revision，并通过 Hadoop CLI 将快照保存为可直接部署的 origin checkpoint。暂不支持 `EXPORT MODEL`。

支持 `text-classification`、`text-generation`、`embedding` 和 `image-embedding`。`/predict` 文本生成接受普通 prompt；文本生成与文本 embedding 还默认提供下述 OpenAI 兼容接口。图片 embedding 仅接受 HTTP/HTTPS URL。

文本分类根据 checkpoint 的 `problem_type` 计算分数：单标签多输出分类使用 softmax，多标签分类和单输出模型使用 sigmoid，均返回得分最高的一个 label/score。明确声明 `problem_type=regression` 的 checkpoint 在服务加载时被拒绝；未声明类型的单输出模型保留 sigmoid 兼容行为。输入列参数不区分大小写，运行配置使用 SQL 声明的字段名。分类、文本 embedding 和生成任务的文本及配置的配对文本字段必须为非空值的 STRING，null、缺字段或非字符串返回 HTTP 400；空字符串仍是有效输入。

## Hugging Face 任务配置

`task` 决定 SQL 输出字段，训练和服务参数不能改变它；如需切换任务，请新建模型。服务启动时也会检查 checkpoint 中的任务是否与服务声明一致。

`task` 和 `repo_id` 必填。输入列必须在 Model 字段列表中声明为 `STRING`，并按任务选择列名配置：

| task | 必填输入配置 | 输出字段 |
| --- | --- | --- |
| `text-classification` | `text_column` | `label`（STRING）、`score`（FLOAT） |
| `text-generation` | `prompt_column` | `generated_text`（STRING） |
| `embedding` | `text_column` | `embedding`（ARRAY&lt;FLOAT&gt;） |
| `image-embedding` | `image_column` | `embedding`（ARRAY&lt;FLOAT&gt;） |

`repo_id` 是 Hub 仓库名称；`revision` 可指定分支、标签或 commit，默认 `main`。固定 commit 可让下载版本更容易复现。下面定义一个文本 embedding 模型：

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

私有仓库可在 TRAIN 参数中通过 `hf_token_secret` 和 `hf_token_secret_key` 引用 Kubernetes Secret。服务仅从 checkpoint 加载模型，不访问 Hub。

## Hugging Face 服务配置

普通推理参数按 `CREATE SERVICE ... WITH` 显式参数、Model 中显式声明的参数、checkpoint 保存的训练参数、内置默认值的顺序取值，前者优先。例如 Model 声明 `pooling=mean`、TRAIN 覆盖为 `cls` 时，未覆盖该参数的服务仍使用 `mean`；如需使用 `cls`，在 SERVICE 的 WITH 中显式设置。仅在 TRAIN 中设置且 Model、SERVICE 均未声明的参数会从 checkpoint 继承。`task` 必须与模型声明和 checkpoint 一致；`trust_remote_code` 使用 checkpoint 记录的值。

在 `CREATE SERVICE ... WITH (...)` 中按需设置：

| 参数 | 默认值 | 用法 |
| --- | --- | --- |
| `device` | `auto` | 可选 `auto`、`cpu`、`cuda` |
| `inference_batch_size` | `8` | 服务内部推理批次大小 |
| `dtype` | `auto` | 可选 `auto`、`float32`、`float16`、`bfloat16` |
| `pod_gpu` | `0` | 请求的 GPU 数量；GPU 推理需设为正数，并准备可用 GPU 节点 |
| `pod_gpu_resource` | `nvidia.com/gpu` | 集群的 GPU 资源名称 |

GPU 推理可设置 `'device' = 'cuda', 'pod_gpu' = '1'`；还需要集群提供匹配的 GPU 驱动、设备插件和运行环境。

## OpenAI 兼容接口

接口默认提供，无需开关。沿用同一个服务、端口和模型实例，原服务 URL 与 `call_service` 继续使用 `/predict`：

| 接口 | task | SQL UDF |
| --- | --- | --- |
| `GET /v1/models` | 文本生成或文本 embedding | 返回该服务的唯一模型别名；其他任务返回空列表 |
| `POST /v1/chat/completions` | `text-generation` | `llm_complete` |
| `POST /v1/embeddings` | `embedding` | `llm_embed_text` |

`model` 必须精确匹配别名，默认是 SQL Model 名称，可在 Model 或 SERVICE 中设置 `served_model_name`。不通过请求切换模型。分类和图片 embedding 暂不提供 OpenAI 推理协议，也不兼容方舟 `ark_embed_image`。

```sql
-- 使用上文已经创建的 embedding 服务。
SELECT llm_embed_text(
    'http://text-embedding-service.<namespace>.svc.cluster.local:80/v1/embeddings',
    '', 'text_embedding_model', title, '{}'
) AS embedding FROM items;

-- 假定 chat_model 已有 origin checkpoint，且 tokenizer 自带 chat template。
CREATE SERVICE `chat-service` ON MODEL chat_model CHECKPOINT = 'v1'
WITH ('served_model_name' = 'article_summary', 'max_new_tokens' = '256');

SELECT llm_complete(
    'http://chat-service.<namespace>.svc.cluster.local:80/v1/chat/completions',
    '', 'article_summary', CONCAT('请总结：', content),
    '{"request":{"temperature":0,"max_completion_tokens":128},"runtime":{"timeout_ms":120000}}'
) AS summary FROM articles;
```

替换 `<namespace>` 为实际命名空间，并保证 SQL/Flink 执行节点能够访问该集群地址。

聊天首版仅支持一条 user 字符串消息，固定非流式、单条结果、不保存请求。使用模型自己的 chat template；缺少模板的普通 prompt 模型继续服务 `/predict`，聊天请求返回 HTTP 400 `chat_template_missing`。不支持多轮、system/developer 消息、工具调用、`reasoning_effort`、`stop`、`max_tokens` 或结构化输出，未知字段返回 400。

`max_completion_tokens` 映射为 `max_new_tokens`，缺省值和硬上限均为服务的 `max_new_tokens`（默认 128）。`temperature=0` 使用 greedy，大于 0 开启采样；仅指定 `top_p<1` 也开启采样。`temperature` 范围为 `[0,2]`，`top_p` 为 `(0,1]`；`temperature=0` 与 `top_p<1` 同时指定时报错。未传采样参数时沿用服务设置。参数覆盖仅作用于当前请求。

embedding 支持字符串或字符串数组，拒绝空白输入；只支持 `encoding_format=float`。保持现有 pooling/normalize 和原始向量维度；`dimensions` 仅允许等于该维度，不提供任意降维。模型所需的 `query:` / `passage:` 等前缀由调用方提供。新接口对超过 `max_length` 的输入返回 400，不静默截断；聊天还检查模板 token 与生成预算之和不超过模型上下文。`usage` 根据实际 token ID 计数。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `served_model_name` | SQL Model 名称 | 接口公开的单个模型别名 |
| `openai_api_key_secret` | 未设置 | K8s Secret 名，注入 `SQLREC_OPENAI_API_KEY`；未配置时可传空密钥 |
| `openai_api_key_secret_key` | `token` | Secret 中的键 |
| `openai_max_request_bytes` | `1048576` | `/v1/*` 请求体上限，字节数 |
| `openai_max_batch_inputs` | `32` | 一次 embedding 请求最大文本数；内部仍按 `inference_batch_size` 分块 |

这些服务参数不从 TRAIN 的 checkpoint 参数继承。配置 Secret 后 `/v1/*` 校验 Bearer token；SQL 第二参数可使用执行上下文注入的 `` `get`('llm_api_key') ``。鉴权不作用于原 `/predict`；该服务保持集群内部使用，对外入口应只转发所需接口。

每 Pod 同时运行一个推理请求，`/predict` 与新接口共用名额，忙时返回 429；通过 replicas 扩容并匹配 SQL 并发。UDF 按行同步请求且不自动重试，限流会使 SQL 调用失败。客户端超时不保证立即停止已启动的推理。新接口错误使用 `error` 对象，参数错误 400、密钥错误 401、未知 model 404、请求过大 413、忙 429、模型未加载 503、内部错误 500。

::: details 按任务调整
文本 embedding 可配置 `pooling`（默认 `mean`）和 `normalize`（默认 `true`）；文本分类可用 `text_pair_column` 指定第二个文本列。文本生成可配置 `max_new_tokens`（默认 `128`）、`do_sample`（默认 `false`）、`temperature` 和 `top_p`。

图片 embedding 的 `image_column` 必须包含 HTTP/HTTPS URL。需要限制下载来源或大小时，可设置 `image_url_allowed_hosts`、`image_download_timeout_ms`（默认 `5000`）、`image_max_bytes`（默认 10 MiB）和 `image_max_pixels`（默认 2000 万）。

`image_url_allowed_hosts` 按完整主机名限制下载来源，重定向也会检查；未配置时允许任意 HTTP/HTTPS 主机。请按图片数据来源配置网络访问和下载限制。
:::
