"""Shared file contracts: explicit features, stable IDs, and strict JSON output."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def captured_result(capture, results):
    """Select an explicit result role; retain the frozen v3 artifact layout."""
    role = capture.get('result_role')
    if role:
        matches = [result for result in results if result.get('role') == role]
        if len(matches) != 1:
            raise ValueError(f'Expected exactly one {role} result')
        return matches[0]
    # v3 plans/artifacts are read as-is, never rewritten or re-fingerprinted.
    return results[-2 if capture['kind'] == 'vectors' else -1]


def stable_id(*values) -> str:
    return hashlib.sha256(json.dumps([str(v) for v in values], separators=(',', ':')).encode()).hexdigest()


def arrow_type(name):
    """Shared SQL/Parquet type mapping; dataset columns remain declared in YAML."""
    import pyarrow as pa
    if name.startswith('ARRAY<'):
        return pa.list_(arrow_type(name[6:-1]))
    return {'STRING': pa.string, 'INT': pa.int32, 'BIGINT': pa.int64,
            'FLOAT': pa.float32, 'DOUBLE': pa.float64, 'BOOLEAN': pa.bool_}[name]()


def digest(path, algorithm='sha256') -> str:
    h = hashlib.new(algorithm)
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.partial')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def read_json(path):
    return json.loads(Path(path).read_text())


def provenance():
    try:
        sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        dirty = bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True))
    except (OSError, subprocess.CalledProcessError):
        sha, dirty = None, None
    versions = {}
    for package in ('numpy', 'pandas', 'pyarrow', 'scikit-learn', 'requests', 'PyYAML',
                    'PyHive', 'thrift', 'thrift_sasl', 'pure-sasl'):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            pass
    # A copied Linux checkout may lack .git; identify executed sources as well.
    sources = [p for p in (ROOT / 'benchmark/model_quality').rglob('*.py')
               if not any(part in ('data', 'runs', 'reports', '__pycache__') for part in p.parts)]
    sources += list((ROOT / 'sqlrec-model/src/main/python').rglob('*.py'))
    sources += list((ROOT / 'sqlrec-model/src/main/java/com/sqlrec/model').rglob('*.java'))
    return {'code_sha': sha, 'dirty_worktree': dirty, 'versions': versions,
            'source_sha256': {str(p.relative_to(ROOT)): digest(p) for p in sorted(sources)}}


def require_columns(frame, columns):
    missing = set(columns) - set(frame.columns)
    if missing:
        raise ValueError(f'Missing columns: {sorted(missing)}')


def binary(values):
    a = np.asarray(values)
    if not np.isin(a, [0, 1]).all():
        raise ValueError('Binary labels must be non-null hard 0/1 values')
    return a.astype(np.int8)


def load_split(data, split):
    if split not in ('train', 'valid', 'test'):
        raise ValueError('split must be train, valid or test')
    data = Path(data)
    manifest = read_json(data / 'manifest.json')
    paths = [data / name for name in manifest['splits'][split]]
    frame = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    row_key = manifest.get('dataset_definition', {}).get('preparation', {}).get('row_key', 'row_id')
    require_columns(frame, [row_key])
    if frame[row_key].isna().any() or frame[row_key].duplicated().any():
        raise ValueError('Null or duplicate observation identity in split')
    return frame


