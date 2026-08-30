from .image_embedding import ImageEmbeddingAdapter
from .text_classification import TextClassificationAdapter
from .text_embedding import TextEmbeddingAdapter
from .text_generation import TextGenerationAdapter

TASK_ADAPTERS = {
    "text-classification": TextClassificationAdapter,
    "text-generation": TextGenerationAdapter,
    "embedding": TextEmbeddingAdapter,
    "image-embedding": ImageEmbeddingAdapter,
}

__all__ = ["TASK_ADAPTERS"]
