"""Compare feature tensors without tolerating changes to embedding indices."""

import numpy as np


def assert_feature_tensors(actual, expected, *, dense, context):
    for suffix in (("values",) if dense else ("values", "lengths")):
        left, right = np.asarray(actual[suffix]), np.asarray(expected[suffix])
        message = f"{context}: {suffix}"
        assert left.shape == right.shape, message
        if dense:
            assert np.isfinite(left).all() and np.isfinite(right).all(), message
            np.testing.assert_allclose(left, right, rtol=1e-6, atol=1e-6, err_msg=message)
            zeros = (left == 0) & (right == 0)
            np.testing.assert_array_equal(np.signbit(left[zeros]), np.signbit(right[zeros]), err_msg=message)
        else:
            np.testing.assert_array_equal(left, right, err_msg=message)
