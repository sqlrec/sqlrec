"""Request formats accepted by the TZRec prediction service."""

import runpy
import unittest
from pathlib import Path


REQUEST_DATA = runpy.run_path(
    str(Path(__file__).resolve().parents[2] / "main/python/tzrec/request_data.py")
)
parse_request_data = REQUEST_DATA["parse_request_data"]
json_to_array_map = REQUEST_DATA["json_to_array_map"]


class TzrecRequestTest(unittest.TestCase):
    def test_row_and_column_formats_produce_the_same_arrow_data(self):
        rows = [{"user": 10, "item": "a"}, {"user": 10, "item": "b"}]
        columnar = {"user": [10], "item": ["a", "b"]}

        self.assertEqual(parse_request_data(rows), parse_request_data(columnar))
        arrays = json_to_array_map(parse_request_data(columnar))
        self.assertEqual(arrays["user"].to_pylist(), [10, 10])
        self.assertEqual(arrays["item"].to_pylist(), ["a", "b"])

    def test_empty_and_single_row_inputs(self):
        self.assertEqual(parse_request_data([]), [])
        self.assertEqual(parse_request_data({}), [])
        self.assertEqual(parse_request_data({"feature": []}), [])
        self.assertEqual(parse_request_data({"feature": [1], "group": ["a"]}),
                         [{"feature": 1, "group": "a"}])

    def test_invalid_request_shapes_are_rejected(self):
        invalid = (
            None,
            "text",
            [1],
            [{"feature": 1}, None],
            {"feature": 1},
            {"feature": [1, 2], "group": ["a", "b", "c"]},
            {"feature": [], "group": ["a"]},
        )
        for request in invalid:
            with self.subTest(request=request):
                with self.assertRaises(ValueError):
                    parse_request_data(request)

    def test_missing_fields_become_null(self):
        rows = [{"user": 1}, {"user": 2, "item": "a"}, {"item": "b"}]
        arrays = json_to_array_map(parse_request_data(rows))

        self.assertEqual(list(arrays), ["user", "item"])
        self.assertEqual(arrays["user"].to_pylist(), [1, 2, None])
        self.assertEqual(arrays["item"].to_pylist(), [None, "a", "b"])

    def test_fields_can_first_appear_after_an_empty_row(self):
        arrays = json_to_array_map([{}, {"item": "a"}])
        self.assertEqual(arrays["item"].to_pylist(), [None, "a"])


if __name__ == "__main__":
    unittest.main()
