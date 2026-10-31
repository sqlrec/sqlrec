"""Independent reference assignments for the benchmark's fixed experiment rules."""
from collections import Counter
from dataclasses import dataclass


@dataclass(frozen=True)
class Experiment:
    name: str
    feature: str
    key: str
    seed: str
    values: tuple[str, str]
    condition: dict
    weights: tuple[float, float] = (.5, .5)
    coverage: float = 1.

    @property
    def ranges(self):
        start = 0.
        ranges = []
        for weight in self.weights:
            ranges.append([start, start + self.coverage * weight])
            start += weight
        return ranges


def experiments(prefix):
    return [Experiment('split', prefix + '_split', prefix + '_split_exp', prefix + '_split_seed',
                       ('control', 'treatment'), {'country': 'US'}),
            Experiment('targeted', prefix + '_targeted', prefix + '_targeted_exp', prefix + '_targeted_seed',
                       ('rank_v1', 'rank_v2'), {'country': 'US', 'age': {'$gte': 18}, 'beta': True},
                       weights=(0., 1.)),
            Experiment('coverage', prefix + '_coverage', prefix + '_coverage_exp', prefix + '_coverage_seed',
                       ('small_a', 'small_b'), {'country': 'US'}, coverage=.5)]


def fnv32a(value):
    # GrowthBook's published hash specification uses UTF-16 code units.
    encoded = value.encode('utf-16-le', errors='surrogatepass')
    result = 2166136261
    for index in range(0, len(encoded), 2):
        result ^= encoded[index] | encoded[index + 1] << 8
        result = result * 16777619 & 0xffffffff
    return result


def bucket(seed, user_id):
    return fnv32a(str(fnv32a(seed + user_id))) % 10000 / 10000


def assignment(experiment, user):
    user_id = user.get('id')
    if not isinstance(user_id, str) or not user_id:
        return None
    if user.get('country') != 'US':
        return None
    if experiment.name == 'targeted':
        age = user.get('age')
        if not isinstance(age, (int, float)) or isinstance(age, bool) or age < 18 or user.get('beta') is not True:
            return None
    value = bucket(experiment.seed, user_id)
    return next((index for index, (start, end) in enumerate(experiment.ranges) if start <= value < end), None)


def users_for(experiments):
    # Select fixed numbers from each coverage class, rather than relying on a
    # statistical assertion that a small random cohort contains both variants.
    coverage = next(exp for exp in experiments if exp.name == 'coverage')
    split = next(exp for exp in experiments if exp.name == 'split')
    groups = {None: [], 0: [], 1: []}
    split_variations = set()
    candidates = []
    for index in range(10000):
        user = {'id': f'gb_user_{index}', 'country': 'US', 'age': 30, 'beta': True}
        candidates.append(user)
        split_variations.add(assignment(split, user))
        group = assignment(coverage, user)
        if len(groups[group]) < 4:
            groups[group].append(user)
        if all(len(rows) == 4 for rows in groups.values()) and split_variations == {0, 1}:
            break
    else:
        raise ValueError('Could not construct complete deterministic coverage fixtures')
    users = [user for rows in groups.values() for user in rows]
    for variation in (0, 1):
        if not any(assignment(split, user) == variation for user in users):
            user = next((user for user in candidates if assignment(split, user) == variation), None)
            if user is None:
                raise ValueError('Could not construct both split variations')
            users.append(user)
    base = users[0]
    users.extend([{**base, 'country': 'CN'}, {**base, 'country': None}, {**base, 'country': 'us'},
                  {**base, 'age': 17}, {**base, 'age': None}, {**base, 'beta': False},
                  {**base, 'beta': None}, {**base, 'id': ''}, {**base, 'id': None}, base])
    return users


def expected(experiments, user):
    tracking, values = [], {}
    for exp in experiments:
        variation = assignment(exp, user)
        values[exp.name + '_value'] = exp.values[variation] if variation is not None else None
        if variation is not None:
            tracking.append((exp.key, str(variation), user['id']))
    # The UDF writes experiment values only, not default/unknown feature values.
    values.update(default_value=None, missing_value=None)
    return tracking, values


def validate_result(experiments, user, rows):
    columns = {'kind', 'experiment_id', 'variation_id', 'user_id', 'tracking_count',
               'split_value', 'targeted_value', 'coverage_value', 'default_value', 'missing_value'}
    if any(set(row) - columns for row in rows):
        raise ValueError('UDF result schema differs from the expected schema')
    # SQLRec's HTTP API omits fields whose SQL value is NULL.
    rows = [{column: row.get(column) for column in columns} for row in rows]
    state = [row for row in rows if row['kind'] == 'state']
    tracking = [row for row in rows if row['kind'] == 'tracking']
    if len(state) != 1 or len(state) + len(tracking) != len(rows):
        raise ValueError('Expected exactly one state row and only tracking rows otherwise')
    expected_tracking, expected_values = expected(experiments, user)
    actual_tracking = [(row['experiment_id'], row['variation_id'], row['user_id']) for row in tracking]
    if Counter(actual_tracking) != Counter(expected_tracking):
        raise ValueError(f'Incorrect experiment tracking for user {user.get("id")!r}: '
                         f'expected {expected_tracking!r}, got {actual_tracking!r}')
    if state[0]['tracking_count'] != len(expected_tracking):
        raise ValueError('Incorrect exposure row count')
    for column, value in expected_values.items():
        if state[0][column] != value:
            raise ValueError(f'Incorrect execution variable {column} for user {user.get("id")!r}: '
                             f'expected {value!r}, got {state[0][column]!r}')
    return {'user': user, 'tracking': actual_tracking, 'values': expected_values}
