"""Local reference and provisioning contracts; no deployed services are contacted."""
from pathlib import Path
from unittest.mock import Mock, patch
import copy
import tempfile
import unittest

import requests

from benchmark.growthbook.checks import assignment, bucket, expected, experiments, fnv32a, users_for, validate_result
from benchmark.growthbook.client import GrowthBook


class EvaluationContractTest(unittest.TestCase):
    def setUp(self):
        self.specs = experiments('gb_contract')
        self.user = {'id': 'alice', 'country': 'US', 'age': 30, 'beta': True}

    def rows(self, user):
        tracking, values = expected(self.specs, user)
        empty = dict(experiment_id=None, variation_id=None, user_id=None, tracking_count=None,
                     split_value=None, targeted_value=None, coverage_value=None, default_value=None, missing_value=None)
        return [{**empty, 'kind': 'state', 'tracking_count': len(tracking), **values},
                *[{**empty, 'kind': 'tracking', 'experiment_id': exp, 'variation_id': variation, 'user_id': uid}
                  for exp, variation, uid in tracking]]

    def test_fnv_reference_uses_known_vectors(self):
        self.assertEqual(fnv32a(''), 0x811c9dc5)
        self.assertEqual(fnv32a('a'), 0xe40c292c)
        self.assertEqual(fnv32a('hello'), 0x4f9f2cab)
        self.assertGreaterEqual(bucket('seed', 'alice'), 0)
        self.assertLess(bucket('seed', 'alice'), 1)

    def test_coverage_ranges_include_gaps_and_exclusive_upper_boundaries(self):
        exp = self.specs[2]
        self.assertEqual(exp.ranges, [[0., .25], [.5, .75]])
        for value, variation in ((0., 0), (.2499, 0), (.25, None), (.5, 1), (.7499, 1), (.75, None)):
            with self.subTest(value=value), patch('benchmark.growthbook.checks.bucket', return_value=value):
                self.assertEqual(assignment(exp, self.user), variation)

    def test_targeting_requires_typed_attributes_and_nonempty_id(self):
        exp = self.specs[1]
        self.assertEqual(assignment(exp, self.user), 1)
        for changes in ({'country': 'CN'}, {'country': None}, {'age': None}, {'age': 17}, {'age': '30'},
                        {'beta': False}, {'beta': None}, {'id': None}, {'id': ''}):
            with self.subTest(changes=changes):
                self.assertIsNone(assignment(exp, {**self.user, **changes}))

    def test_cohort_guarantees_both_variants_and_coverage_exclusion(self):
        users = users_for(self.specs)
        self.assertEqual({assignment(self.specs[0], user) for user in users}, {0, 1, None})
        self.assertEqual({assignment(self.specs[2], user) for user in users}, {0, 1, None})

    def test_tracking_duplicates_wrong_ids_and_wrong_context_values_fail(self):
        rows = self.rows(self.user)
        validate_result(self.specs, self.user, rows[::-1])
        invalid = []
        invalid.append(rows + [rows[1]])
        wrong_id = copy.deepcopy(rows)
        wrong_id[1]['experiment_id'] = 'unrelated'
        invalid.append(wrong_id)
        wrong_variable = copy.deepcopy(rows)
        wrong_variable[0]['targeted_value'] = 'rank_v1'
        invalid.append(wrong_variable)
        invalid.append(rows[1:])
        for result in invalid:
            with self.subTest(result=result), self.assertRaises(ValueError):
                validate_result(self.specs, self.user, result)

    def test_excluded_user_does_not_get_default_values_or_previous_assignments(self):
        user = {**self.user, 'country': None}
        rows = self.rows(user)
        validate_result(self.specs, user, rows)
        self.assertEqual(len(rows), 1)
        for column in ('split_value', 'targeted_value', 'coverage_value', 'default_value', 'missing_value'):
            changed = copy.deepcopy(rows)
            changed[0][column] = 'stale_or_default'
            with self.subTest(column=column), self.assertRaises(ValueError):
                validate_result(self.specs, user, changed)

    def test_http_response_can_omit_null_fields(self):
        rows = [{key: value for key, value in row.items() if value is not None}
                for row in self.rows(self.user)]
        validate_result(self.specs, self.user, rows)
        rows[0]['unexpected'] = 'value'
        with self.assertRaisesRegex(ValueError, 'schema'):
            validate_result(self.specs, self.user, rows)


class APIContractTest(unittest.TestCase):
    def test_provision_creates_real_experiments_and_links_their_returned_variation_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            api = GrowthBook('http://unused', Path(directory), 'gb_contract')
            self.addCleanup(api.close)
            api.owner = 'user_test'
            specs = experiments('gb_contract')

            def respond(method, path, payload=None, **kwargs):
                if path.endswith('/projects'):
                    return {'project': {'id': 'prj_test'}}
                if path.endswith('/experiments'):
                    return {'experiment': {'id': payload['trackingKey'],
                                           'variations': [{'variationId': 'var_server_a'},
                                                          {'variationId': 'var_server_b'}]}}
                if path.endswith('/sdk-connections'):
                    return {'sdkConnection': {'id': 'sdk_test', 'key': 'sdk_public_key'}}
                return {}

            api.request = Mock(side_effect=respond)
            self.assertEqual(api.provision(specs), 'sdk_public_key')
            calls = api.request.call_args_list
            created = [call.args[2] for call in calls if call.args[1] == '/api/v1/experiments']
            linked = [call.args[2] for call in calls if call.args[1] == '/api/v1/features'][:3]
            self.assertEqual(len(created), 3)
            for spec, experiment, feature in zip(specs, created, linked):
                self.assertEqual(experiment['trackingKey'], spec.key)
                self.assertEqual(experiment['hashVersion'], 2)
                self.assertEqual(feature['owner'], 'user_test')
                rule = feature['environments']['production']['rules'][0]
                self.assertEqual(rule['type'], 'experiment-ref')
                self.assertEqual(rule['experimentId'], spec.key)
                self.assertEqual([v['variationId'] for v in rule['variations']], ['var_server_a', 'var_server_b'])

    def test_uncertain_creation_is_not_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            api = GrowthBook('http://unused', Path(directory), 'gb_contract')
            self.addCleanup(api.close)
            api.token = 'test_token'
            api.session.request = Mock(side_effect=requests.ConnectionError('lost response'))
            with self.assertRaisesRegex(RuntimeError, 'writes are not retried'):
                api.request('POST', '/api/v1/experiments', {})
            api.session.request.assert_called_once()
            self.assertEqual(api.resources['uncertain_writes'], [{'method': 'POST', 'path': '/api/v1/experiments'}])

    def test_cleanup_continues_after_a_failure_and_revokes_the_temporary_key(self):
        with tempfile.TemporaryDirectory() as directory:
            api = GrowthBook('http://unused', Path(directory), 'gb_contract')
            self.addCleanup(api.close)
            api.token = 'temporary_secret'
            api.secrets.append(api.token)
            api.remember('DELETE', '/api/v1/projects/owned')
            api.remember('DELETE', '/api/v1/features/owned')
            api.request = Mock(side_effect=[RuntimeError('delete failed'), {}, {}])
            with self.assertRaisesRegex(RuntimeError, 'delete failed'):
                api.cleanup()
            self.assertEqual(api.request.call_count, 3)
            self.assertEqual(api.request.call_args.args, ('DELETE', '/keys', {'key': 'temporary_secret'}))
            self.assertNotIn('temporary_secret', (Path(directory) / 'resources.json').read_text())

    def test_cleanup_archives_features_before_deleting_them(self):
        with tempfile.TemporaryDirectory() as directory:
            api = GrowthBook('http://unused', Path(directory), 'gb_contract')
            self.addCleanup(api.close)
            api.remember('DELETE', '/api/v1/features/owned')
            api.request = Mock(return_value={})
            api.cleanup()
            self.assertEqual(api.request.call_args_list[0].args,
                             ('POST', '/api/v1/features/owned', {'archived': True}))
            self.assertEqual(api.request.call_args_list[1].args, ('DELETE', '/api/v1/features/owned'))


if __name__ == '__main__':
    unittest.main()
