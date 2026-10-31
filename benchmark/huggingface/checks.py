"""Output and row-identity checks; no semantic quality claims for random models."""
import math


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def check_predictions(case, inputs, predictions):
    expected = {row['row_id'] for row in inputs}
    ids = [row.get('row_id') for row in predictions]
    if (len(predictions) != len(inputs) or not all(isinstance(value, str) for value in ids)
            or len(set(ids)) != len(ids) or set(ids) != expected):
        raise ValueError('Prediction row IDs are missing, duplicated or unexpected')
    for row in predictions:
        if set(row) != {'row_id', *case.output_columns}:
            raise ValueError('Prediction columns differ from the declared task output')
        if case.task == 'text-classification':
            if row['label'] not in ('LABEL_0', 'LABEL_1'):
                raise ValueError('Classification returned an unknown label')
            if not finite(row['score']) or not 0 <= row['score'] <= 1:
                raise ValueError('Classification score must be finite and in [0, 1]')
        elif case.task == 'text-generation':
            # An immediate EOS can legitimately produce an empty completion.
            if not isinstance(row['generated_text'], str):
                raise ValueError('Generation must return a STRING completion')
        else:
            vector = row['embedding']
            if (not isinstance(vector, list) or len(vector) != case.embedding_dimension
                    or not all(finite(value) for value in vector)):
                raise ValueError('Embedding has the wrong dimension or nonfinite values')
            norm = math.sqrt(sum(value * value for value in vector))
            if not math.isclose(norm, 1, abs_tol=1e-4):
                raise ValueError('Embedding is not L2 normalized')
    return {row['row_id']: row for row in predictions}


def check_equivalent(case, expected, actual):
    if expected.keys() != actual.keys():
        raise ValueError('Prediction identities changed between requests')
    for row_id, previous in expected.items():
        current = actual[row_id]
        for column in case.output_columns:
            left, right = previous[column], current[column]
            if column in ('score', 'embedding'):
                left = left if isinstance(left, list) else [left]
                right = right if isinstance(right, list) else [right]
                if len(left) != len(right) or any(
                        not math.isclose(a, b, rel_tol=1e-4, abs_tol=1e-5) for a, b in zip(left, right)):
                    raise ValueError(f'{column} changed between single, batched or repeated requests')
            elif left != right:
                raise ValueError(f'{column} changed between single, batched or repeated requests')
