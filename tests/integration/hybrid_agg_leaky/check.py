import sys

expected_fn = sys.argv[1]
actual_fn = sys.argv[2]
with open(expected_fn) as f_expected, open(actual_fn) as f_actual:
    expected = set(f_expected.read().split())
    actual = set(f_actual.read().split())
    print(expected)
    assert expected == actual, "expected " + str(expected) + " but was " + str(actual)
    print("All ok")