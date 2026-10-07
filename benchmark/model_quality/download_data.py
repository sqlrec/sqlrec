"""Download pinned official archives; extract only the benchmark's input files."""
from __future__ import annotations

import argparse
import shutil
import sys
import tarfile
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

import requests

from .common import digest, write_json
from .user_workflow.configuration import read_dataset_definition
from .user_workflow.specs import identifier


def download(dataset, output, config_path=None):
    if config_path is None:
        path = Path(__file__).parent / 'configs/datasets' / (identifier(dataset) + '.yaml')
    else:
        path = Path(config_path)
    config, metadata = read_dataset_definition(path)
    if dataset is not None and config['id'] != dataset:
        raise ValueError('--dataset differs from the dataset YAML identity')
    if 'source' not in config:
        raise ValueError('Dataset YAML must declare a download source')
    spec = config['source']
    required = set(spec['files'].values())
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    archive = output / Path(urlsplit(spec['url']).path).name
    algorithm = 'sha256' if 'sha256' in spec else 'md5'
    expected = spec[algorithm]
    if not archive.exists() or digest(archive, algorithm) != expected:
        temporary = archive.with_suffix(archive.suffix + '.partial')
        for attempt in range(3):
            offset = temporary.stat().st_size if temporary.exists() else 0
            try:
                with requests.get(spec['url'], headers={'Range': f'bytes={offset}-'} if offset else {},
                                  stream=True, timeout=(15, 120)) as response:
                    response.raise_for_status()
                    resume = offset > 0 and response.status_code == 206
                    if resume and not response.headers.get('Content-Range', '').startswith(f'bytes {offset}-'):
                        raise ValueError('Invalid range response')
                    with temporary.open('ab' if resume else 'wb') as stream:
                        for chunk in response.iter_content(1024 * 1024):
                            stream.write(chunk)
                if digest(temporary, algorithm) != expected:
                    temporary.unlink()
                    raise ValueError('Official archive checksum mismatch')
                temporary.replace(archive)
                break
            except (requests.RequestException, ValueError) as error:
                if attempt == 2:
                    raise
                print(f'Download interrupted; retrying ({attempt + 1}/3): {type(error).__name__}', file=sys.stderr)
    found = set()
    # Flatten only explicitly selected regular files; never extract paths/symlinks.
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as pack:
            for member in pack.infolist():
                name = Path(member.filename).name
                if name in required and not member.is_dir():
                    if name in found:
                        raise ValueError(f'Duplicate archive member: {name}')
                    with pack.open(member) as src, (output / name).open('wb') as dst:
                        shutil.copyfileobj(src, dst)
                    found.add(name)
    else:
        with tarfile.open(archive) as pack:
            for member in pack:
                name = Path(member.name).name
                if name in required and member.isfile():
                    if name in found:
                        raise ValueError(f'Duplicate archive member: {name}')
                    with pack.extractfile(member) as src, (output / name).open('wb') as dst:
                        shutil.copyfileobj(src, dst)
                    found.add(name)
    if found != required:
        raise ValueError(f'Missing archive inputs: {required - found}')
    write_json(output / 'download.json', {'dataset': config['id'], 'spec': spec, **metadata, 'archive_sha256': digest(archive),
                                        'files': {name: digest(output / name) for name in sorted(found)}})
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument('--dataset', help='Load configs/datasets/<ID>.yaml')
    selection.add_argument('--config', help='Explicit dataset YAML, also used by preparation and experiments')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    print(download(args.dataset, args.output, args.config))
