"""Pinned public test checkpoints and small, deterministic input fixtures."""
from dataclasses import dataclass

from benchmark.model_quality.config import Feature


@dataclass(frozen=True)
class Case:
    name: str
    task: str
    repo_id: str
    revision: str
    fields: tuple[Feature, ...]
    parameters: dict
    output_columns: tuple[str, ...]
    embedding_dimension: int | None = None


BERT = 'hf-internal-testing/tiny-random-bert'
BERT_REVISION = 'f171d7baecaf37b5da5a3616d8833b9969753535'
TASKS = ('text-classification', 'text-generation', 'embedding', 'image-embedding')
# Only fetch the runtime's safetensors weights, configuration and processors.
# In particular, exclude TensorFlow, pickle, ONNX and duplicate weight formats.
ALLOW_PATTERNS = ','.join((
    'config.json', 'generation_config.json', 'model.safetensors',
    'tokenizer.json', 'tokenizer_config.json', 'special_tokens_map.json',
    'vocab.txt', 'vocab.json', 'merges.txt', 'preprocessor_config.json',
))
CASES = (
    Case('classification', 'text-classification', BERT, BERT_REVISION,
         (Feature('body', 'STRING'),), {'text_column': 'BODY'}, ('label', 'score')),
    Case('classification_pair', 'text-classification', BERT, BERT_REVISION,
         (Feature('body', 'STRING'), Feature('pair', 'STRING')),
         {'text_column': 'BODY', 'text_pair_column': 'PAIR'}, ('label', 'score')),
    Case('generation', 'text-generation', 'hf-internal-testing/tiny-random-gpt2',
         '71034c5d8bde858ff824298bdedc65515b97d2b9', (Feature('prompt', 'STRING'),),
         {'prompt_column': 'PROMPT', 'max_new_tokens': 8, 'do_sample': 'false'}, ('generated_text',)),
    Case('embedding', 'embedding', BERT, BERT_REVISION,
         (Feature('body', 'STRING'),),
         {'text_column': 'BODY', 'pooling': 'mean', 'normalize': 'true'}, ('embedding',), 32),
    Case('image_embedding', 'image-embedding', 'hf-internal-testing/tiny-random-vit',
         '96e253fe90cfcededfa8edf8b7e6f230f87a63a1', (Feature('image_url', 'STRING'),),
         {'image_column': 'IMAGE_URL', 'pooling': 'cls', 'normalize': 'true',
          'image_download_timeout_ms': 30000}, ('embedding',), 32),
)

IMAGE_REVISION = '9dd8e4710f6b2ddf5184d308639e0359bc7b55be'
IMAGE_BASE = ('https://huggingface.co/datasets/huggingface/documentation-images/resolve/'
              + IMAGE_REVISION)
IMAGE_URLS = (IMAGE_BASE + '/coco_sample.png', IMAGE_BASE + '/hub/parrots.png')


def input_rows(case):
    # Three rows cross inference_batch_size=2, including a final partial batch.
    # Rows 0 and 2 have identical inputs and different identities.
    values = ('SQLRec is useful.', 'A longer sentence tests padding in a mixed batch.', 'SQLRec is useful.')
    if case.task == 'image-embedding':
        values = (IMAGE_URLS[0], IMAGE_URLS[1], IMAGE_URLS[0])
    column = case.fields[0].name
    rows = [{'row_id': f'row_{index}', column: value} for index, value in enumerate(values)]
    if len(case.fields) == 2:
        for row in rows:
            row['pair'] = 'This is a paired sentence.'
    return rows
