"""Create real GrowthBook experiments and check get_growthbook_features via SQLRec."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import argparse
import logging
import os
import signal
import time
import uuid

from benchmark.model_quality.client import APIClient, HS2Client, SQLFailed
from benchmark.model_quality.common import literal, write_json

from .checks import experiments, users_for, validate_result
from .client import GrowthBook


def endpoints():
    host = os.environ.get('NODE_IP')
    if not host:
        raise ValueError('Source deploy/env.sh first (NODE_IP is missing)')
    sql = {'host': host, 'port': int(os.environ.get('SQLREC_THRIFT_PORT', 30000)), 'auth': 'NOSASL'}
    api = {'base_url': f'http://{host}:{os.environ.get("SQLREC_REST_PORT", 30001)}'}
    if os.environ.get('AUTH_ENABLED', 'true').lower() == 'true':
        credentials = {'username_env': 'AUTH_USERNAME', 'password_env': 'AUTH_PASSWORD'}
        sql.update(auth='CUSTOM', **credentials)
        api.update(**credentials)
    return sql, api


def register_sql(execute, prefix, sdk_host, sdk_key, specs):
    feature_keys = [exp.feature for exp in specs] + [prefix + '_default', prefix + '_missing']
    arguments = ',\n    '.join(literal(key) for key in feature_keys)
    variables = ',\n    '.join(f'`get`({literal(key)}) AS {name}_value'
                               for key, name in zip(feature_keys, ('split', 'targeted', 'coverage', 'default', 'missing')))
    null_values = ', '.join(f'CAST(NULL AS VARCHAR) AS {name}_value'
                            for name in ('split', 'targeted', 'coverage', 'default', 'missing'))
    statements = [
        f'CREATE SQL FUNCTION `{prefix}`',
        'DEFINE INPUT TABLE observations (id STRING, country STRING, age INT, beta BOOLEAN)',
        'CACHE TABLE tracking_schema AS SELECT CAST(NULL AS VARCHAR) AS experiment_id,\n'
        '    CAST(NULL AS VARCHAR) AS variation_id, CAST(NULL AS VARCHAR) AS user_id LIMIT 0',
        f'CACHE TABLE tracking AS CALL get_growthbook_features(\n    {literal(sdk_host)},\n'
        f'    {literal(sdk_key)}, observations,\n    {arguments}\n) LIKE tracking_schema',
        # COUNT produces one state row even when nobody entered an experiment.
        # Reading tracking first also makes variable evaluation depend on the UDF.
        'CACHE TABLE totals AS SELECT COUNT(*) AS tracking_count FROM tracking',
        'CACHE TABLE udf_result AS SELECT \'state\' AS kind, CAST(NULL AS VARCHAR) AS experiment_id,\n'
        '    CAST(NULL AS VARCHAR) AS variation_id, CAST(NULL AS VARCHAR) AS user_id, tracking_count,\n'
        f'    {variables}\nFROM totals\nUNION ALL\n'
        'SELECT \'tracking\' AS kind, experiment_id, variation_id, user_id,\n'
        f'    CAST(NULL AS BIGINT) AS tracking_count, {null_values}\nFROM tracking',
        'RETURN udf_result',
        f'CREATE API `{prefix}` WITH `{prefix}`',
    ]
    for statement in statements:
        execute(statement)


def exercise(specs, prefix, api_config, root):
    users = users_for(specs)
    evidence = []
    durations = []
    api = APIClient(api_config)

    def payload(rows):
        # LIKE permits empty-table recovery by default. Tests must see UDF
        # failures, including invalid user-table sizes, instead of empty results.
        return {'data': {'observations': rows}, 'params': {'IGNORE_UNION_EXCEPTION': False}}

    try:
        for user in users:
            start = time.monotonic()
            rows = api.call(prefix, payload([user]), retry=False)
            durations.append(time.monotonic() - start)
            evidence.append(validate_result(specs, user, rows))
        # The cached feature repository must keep attributes isolated across
        # simultaneous requests, including excluded users with the same ID.
        concurrent_users = [users[0], {**users[0], 'country': 'CN'}, users[1], {**users[1], 'beta': False}]

        def evaluate(user):
            client = APIClient(api_config)
            try:
                return validate_result(specs, user, client.call(
                    prefix, payload([user]), retry=False))
            finally:
                client.close()

        with ThreadPoolExecutor(max_workers=4) as pool:
            evidence.extend(pool.map(evaluate, concurrent_users))
        for invalid_rows in ([], [users[0], users[1]]):
            try:
                api.call(prefix, payload(invalid_rows), retry=False)
            except SQLFailed:
                continue
            raise ValueError('The UDF accepted a user table that did not contain exactly one row')
        rows = api.call(prefix, payload([users[0]]), retry=False)
        evidence.append(validate_result(specs, users[0], rows))
    finally:
        api.close()
        write_json(root / 'predictions.json', evidence)
    return {'checked_requests': len(evidence), 'invalid_requests_rejected': 2,
            'timed_requests': len(durations), 'mean_request_ms': 1000 * sum(durations) / len(durations)}


def write_report(root, result):
    write_json(root / 'results.json', result)
    metrics = result.get('metrics', {})
    lines = ['# GrowthBook UDF integration benchmark', '',
             f'Status: {result["status"]}', '',
             '| Check | Result |', '| --- | --- |',
             f'| Single-user, repeated and concurrent requests | {metrics.get("checked_requests", "—")} |',
             f'| Invalid user-table sizes rejected | {metrics.get("invalid_requests_rejected", "—")} |',
             f'| Mean timed API request (ms) | {metrics.get("mean_request_ms", 0):.2f} |',
             f'| GrowthBook resource cleanup | {result.get("growthbook_cleanup", "pending")} |',
             f'| SQL function/API cleanup | {result.get("sql_cleanup", "pending")} |', '',
             'Assignments are checked against an independent FNV-1a/hash-v2 reference,',
             'including experiment IDs, numeric variation indexes, raw bucketing IDs and execution variables.', '']
    for field in ('error', 'cleanup_error'):
        if result.get(field):
            lines.append(f'- {field}: {result[field]}')
    (root / 'summary.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path(__file__).parent / 'runs')
    args = parser.parse_args()
    prefix = 'gb_' + uuid.uuid4().hex[:16]
    root = args.output.resolve() / (datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '_' + prefix)
    root.mkdir(parents=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler(root / 'run.log'), logging.StreamHandler()])
    result = {'status': 'FAILED', 'prefix': prefix}
    specs = experiments(prefix)
    sql = gb = None
    sql_attempted = False
    error = None

    def execute(statement):
        with (root / 'statements.sql').open('a', encoding='utf-8') as stream:
            stream.write(statement.rstrip(';') + ';\n\n')
        return sql.execute(statement, timeout=120)

    try:
        sql_config, api_config = endpoints()
        host = os.environ.get('GROWTHBOOK_BENCH_API_HOST',
                              f'http://{os.environ["NODE_IP"]}:{os.environ.get("GROWTHBOOK_API_PORT", 30032)}')
        gb = GrowthBook(host, root, prefix, int(os.environ.get('GROWTHBOOK_BENCH_TIMEOUT', 120)))
        gb.wait_ready()
        gb.authenticate()
        sdk_key = gb.provision(specs)
        gb.wait_payload(sdk_key, specs)
        sdk_host = os.environ.get('GROWTHBOOK_BENCH_SDK_HOST', host)
        write_json(root / 'config.json', {'growthbook_api': host, 'growthbook_sdk': sdk_host,
                                         'sql': sql_config, 'api': api_config,
                                         'experiments': [exp.__dict__ for exp in specs]})
        sql = HS2Client(sql_config).connect()
        execute('SET table.sql-dialect = default')
        sql_attempted = True
        register_sql(execute, prefix, sdk_host, sdk_key, specs)
        result['metrics'] = exercise(specs, prefix, api_config, root)
    except BaseException as failure:
        error = failure
    finally:
        previous = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM)}
        cleanup_errors = []
        try:
            if sql_attempted:
                try:
                    if sql.transport_broken:
                        sql.close()
                        sql = HS2Client(sql_config).connect()
                    execute(f'DROP API IF EXISTS `{prefix}`')
                    execute(f'DROP SQL FUNCTION IF EXISTS `{prefix}`')
                    result['sql_cleanup'] = 'completed'
                except Exception as failure:
                    result['sql_cleanup'] = 'failed'
                    cleanup_errors.append(str(failure))
            else:
                result['sql_cleanup'] = 'not_created'
            if gb:
                try:
                    gb.cleanup()
                    result['growthbook_cleanup'] = 'completed'
                except Exception as failure:
                    result['growthbook_cleanup'] = 'failed'
                    cleanup_errors.append(str(failure))
            if cleanup_errors:
                result['cleanup_error'] = gb.redact('; '.join(cleanup_errors)) if gb else '; '.join(cleanup_errors)
                error = error or RuntimeError(result['cleanup_error'])
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            if sql:
                try:
                    sql.close()
                except Exception as failure:
                    cleanup_errors.append(str(failure))
                    result['cleanup_error'] = gb.redact('; '.join(cleanup_errors)) if gb else '; '.join(cleanup_errors)
                    error = error or failure
            if gb:
                gb.close()
        if error:
            result['error'] = gb.redact(f'{type(error).__name__}: {error}') if gb else str(error)
            logging.error('%s', result['error'])
        else:
            result['status'] = 'COMPLETED'
        write_report(root, result)
        logging.info('Report: %s', root / 'summary.md')
    return 130 if isinstance(error, KeyboardInterrupt) else 1 if error else 0


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
