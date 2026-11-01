"""Real serving HTTP contracts, followed by SQL LLM UDF calls to the same model."""
from contextlib import contextmanager
import re
import subprocess
import time

import requests

from benchmark.model_quality.common import literal, write_json
from benchmark.model_quality.config import Feature
from benchmark.model_quality.data import fields
from .checks import check_equivalent, check_predictions, finite
from .config import input_rows


@contextmanager
def forwarded_service(resources, service):
    """Use kubectl's ephemeral loopback port; always reap the owned forwarder."""
    log_path = resources.root / 'openai-port-forward.log'
    process = None
    with log_path.open('w') as log:
        try:
            process = subprocess.Popen([resources.env.kubectl, '-n', resources.workloads.namespace,
                                        'port-forward', '--address=127.0.0.1', 'service/' + service, ':80'],
                                       stdout=log, stderr=log)
            deadline = time.monotonic() + 30
            while True:
                if process.poll() is not None:
                    raise RuntimeError('OpenAI service port-forward exited; see openai-port-forward.log')
                match = re.search(r'Forwarding from 127\.0\.0\.1:(\d+) ->', log_path.read_text())
                if match:
                    yield 'http://127.0.0.1:' + match.group(1)
                    return
                if time.monotonic() >= deadline:
                    raise RuntimeError('OpenAI service port-forward timed out')
                time.sleep(.1)
        finally:
            if process is not None:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def usage(result, completion=False):
    value = result.get('usage', {})
    keys = ('prompt_tokens', 'completion_tokens', 'total_tokens') if completion else ('prompt_tokens', 'total_tokens')
    if any(type(value.get(key)) is not int or value[key] < 0 for key in keys):
        raise ValueError('Invalid OpenAI token usage')
    if value['prompt_tokens'] <= 0 or value['total_tokens'] != value['prompt_tokens'] + value.get('completion_tokens', 0):
        raise ValueError('Inconsistent OpenAI token usage')


def completion(result, model, budget):
    if (result.get('model') != model or result.get('object') != 'chat.completion'
            or not isinstance(result.get('id'), str) or not result['id'].startswith('chatcmpl-')
            or type(result.get('created')) is not int):
        raise ValueError('Invalid Chat Completions envelope')
    choices = result.get('choices')
    if not isinstance(choices, list) or len(choices) != 1:
        raise ValueError('Expected exactly one chat completion')
    choice = choices[0]
    message = choice.get('message', {})
    if (type(choice.get('index')) is not int or choice['index'] != 0 or choice.get('finish_reason') not in ('stop', 'length')
            or message.get('role') != 'assistant' or not isinstance(message.get('content'), str)):
        raise ValueError('Invalid chat completion choice')
    usage(result, completion=True)
    if result['usage']['completion_tokens'] > budget:
        raise ValueError('Completion exceeded requested token budget')
    return message['content']


def vectors(result, model, size, dimension):
    if result.get('object') != 'list' or result.get('model') != model:
        raise ValueError('Invalid embeddings envelope')
    data = result.get('data')
    if not isinstance(data, list) or len(data) != size:
        raise ValueError('Invalid embeddings count')
    values = []
    for index, item in enumerate(data):
        vector = item.get('embedding')
        if (type(item.get('index')) is not int or item['index'] != index or item.get('object') != 'embedding'
                or not isinstance(vector, list) or len(vector) != dimension or not all(finite(v) for v in vector)):
            raise ValueError('Invalid embeddings index, dimension or values')
        values.append(vector)
    usage(result)
    return values


def register_udf_api(resources, case, endpoint):
    function = resources.name + '_openai'
    udf = 'llm_complete' if case.task == 'text-generation' else 'llm_embed_text'
    settings = '{"request":{"temperature":0,"max_completion_tokens":8}}' if udf == 'llm_complete' else '{}'
    column = case.fields[0].name
    expression = (f'{udf}({literal(endpoint)}, \'\', {literal(resources.name)}, `{column}`, '
                  f'{literal(settings)}) AS `{case.output_columns[0]}`')
    for sql in (f'CREATE SQL FUNCTION `{function}`',
                f'DEFINE INPUT TABLE observations ({fields([Feature("row_id", "STRING"), *case.fields])})',
                f'CACHE TABLE predictions AS SELECT row_id, {expression} FROM observations',
                'RETURN predictions', f'CREATE API `{function}` WITH `{function}`'):
        resources.execute(sql)
    return function


def exercise_openai(case, resources, api, service):
    inputs, model = input_rows(case), resources.name
    path = '/v1/chat/completions' if case.task == 'text-generation' else '/v1/embeddings'
    cluster = f'http://{service}.{resources.workloads.namespace}.svc.cluster.local:80'
    checked = 0
    with resources.stage('openai_checks'), forwarded_service(resources, service) as local, requests.Session() as session:
        session.trust_env = False  # Loopback forwarding must not pass through an HTTP proxy.

        def call(route, body=None, status=200):
            nonlocal checked
            response = session.request('GET' if body is None else 'POST', local + route, json=body, timeout=120)
            if response.status_code != status:
                raise ValueError(f'OpenAI {route}: expected HTTP {status}, got {response.status_code}')
            result = response.json()
            if status != 200 and (not isinstance(result.get('error'), dict) or not result['error'].get('code')):
                raise ValueError('OpenAI rejection did not use the error envelope')
            checked += 1
            return result

        models = call('/v1/models')
        if models.get('object') != 'list' or [item.get('id') for item in models.get('data', [])] != [model]:
            raise ValueError('OpenAI model alias differs from the SQL model')
        if case.task == 'text-generation':
            body = {'model': model, 'messages': [{'role': 'user', 'content': inputs[0]['prompt']}],
                    'stream': False, 'n': 1, 'store': False, 'temperature': 0, 'max_completion_tokens': 8}
        else:
            body = {'model': model, 'input': inputs[0]['body'], 'encoding_format': 'float'}
        call(path, {**body, 'model': 'unknown-model'}, 404)
        call('/v1/embeddings' if case.task == 'text-generation' else '/v1/chat/completions', body, 400)
        if case.task == 'text-generation' and not case.chat_template:
            rejection = call(path, body, 400)
            if rejection['error']['code'] != 'chat_template_missing':
                raise ValueError('Plain prompt model did not report the missing chat template')
            return {'http_checks': checked, 'chat_template_missing_verified': True}

        reference = []
        responses = []
        for row in inputs:
            current = ({**body, 'messages': [{'role': 'user', 'content': row['prompt']}]} if case.chat_template
                       else {**body, 'input': row['body']})
            result = call(path, current)
            responses.append(result)
            value = (completion(result, model, 8) if case.chat_template
                     else vectors(result, model, 1, case.embedding_dimension)[0])
            reference.append({'row_id': row['row_id'], case.output_columns[0]: value})
        expected = check_predictions(case, inputs, reference)
        if case.chat_template:
            for extra in ({'stream': True}, {'n': 2}, {'store': True}, {'reasoning_effort': 'low'},
                          {'max_completion_tokens': 9}, {'temperature': -1}, {'top_p': 0},
                          {'messages': [{'role': 'system', 'content': 'invalid'}]}):
                call(path, {**body, **extra}, 400)
            result = call(path, {**body, 'max_completion_tokens': 1})
            completion(result, model, 1)
        else:
            batch = call(path, {**body, 'input': [row['body'] for row in inputs], 'dimensions': case.embedding_dimension})
            values = vectors(batch, model, len(inputs), case.embedding_dimension)
            check_equivalent(case, expected, check_predictions(case, inputs, [
                {'row_id': row['row_id'], 'embedding': value} for row, value in zip(inputs, values)]))
            if batch['usage']['prompt_tokens'] != sum(value['usage']['prompt_tokens'] for value in responses):
                raise ValueError('Batch token usage differs from single inputs')
            for extra in ({'dimensions': case.embedding_dimension - 1}, {'encoding_format': 'base64'},
                          {'input': ''}, {'input': []}, {'input': None}, {'input': [1, 2]}, {'input': ['x'] * 33}):
                call(path, {**body, **extra}, 400)
            # Original /predict vectors must match the protocol adapter exactly.
            native = call('/predict', [{case.fields[0].name: row[case.fields[0].name]} for row in inputs])
            check_equivalent(case, expected, check_predictions(case, inputs, [
                {'row_id': row['row_id'], 'embedding': value} for row, value in zip(inputs, native['embedding'])]))
        long = 'very long input ' * 1000
        call(path, {**body, **({'messages': [{'role': 'user', 'content': long}]} if case.chat_template else {'input': long})}, 400)
        function = register_udf_api(resources, case, cluster + path)
        for rows in (inputs, list(reversed(inputs))):
            actual = api.call(function, {'data': {'observations': rows}}, retry=False)
            check_equivalent(case, expected, check_predictions(case, rows, actual))
        # Ensure rejections and a different request budget do not change defaults.
        result = call(path, body)
        value = (completion(result, model, 8) if case.chat_template
                 else vectors(result, model, 1, case.embedding_dimension)[0])
        check_equivalent(case, {'row_0': expected['row_0']}, {'row_0': {'row_id': 'row_0', case.output_columns[0]: value}})
        write_json(resources.root / 'openai-responses.json', responses)
        return {'http_checks': checked, 'udf_requests': 2, 'udf_rows': len(inputs) * 2,
                'protocol_and_udf_equivalent': True}
