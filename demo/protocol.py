import sys
import json
import conclave.lang as cc
from conclave.utils import *
from conclave import workflow


def protocol():
    # diagnosis_col = "12"
    # num_diagnosis_cols = 13

    # left_diagnosis_cols = [defCol(str(i), "INTEGER", 1) for i in range(num_diagnosis_cols)]
    # left_diagnosis = cc.create("left_diagnosis", left_diagnosis_cols, {1})

    # right_diagnosis_cols = [defCol(str(i), "INTEGER", 2) for i in range(num_diagnosis_cols)]
    # right_diagnosis = cc.create("right_diagnosis", right_diagnosis_cols, {2})

    # cohort = cc.concat([left_diagnosis, right_diagnosis], "cohort")
    # counts = cc.aggregate_count(cohort, "counts", [diagnosis_col], "total")
    # # cc.collect(counts, 1)
    # cc.collect(cc.sort_by(counts, "actual", "total"), 1)

    # return {left_diagnosis, right_diagnosis}

	cols_in_one = [
		defCol("car_id", "INTEGER", [1]),
		defCol("location", "INTEGER", [1])
	]
	in_one = cc.create("in1", cols_in_one, {1})

	cols_in_two = [
		defCol("car_id", "INTEGER", [2]),
		defCol("location", "INTEGER", [2])
	]
	in_two = cc.create("in2", cols_in_two, {2})

	# cols_in_three = [
	# 	defCol("car_id", "LOL", [3]),
	# 	defCol("location", "LOL", [3])
	# ]
	# in_three = cc.create("in3", cols_in_three, {3})

	# u = union(in_one, in_three, "unioned", "car_id", "car_id")
	# combined = concat([in_one, in_two, in_three], "combined_data")

	# j1 = join(in_one, in_two, "asdf", ["car_id"], ["car_id"])
	# mult = multiply(in_three, "multiplied_three", "location", ["location", 100])
	# u2 = join(j1, mult, "u2", ["car_id"], ["car_id"])
	# mult = multiply(combined, "mult", "location", ["location", 100])
	# selected = project(combined, "selected", ["car_id", "location"])
	# collect(u2, 1)


	combined = cc.concat([in_one, in_two], "combined_data1")
	# combined2 = cc.concat([in_three, in_two], "combined_data2")
	# combined = cc.concat([combined1, combined2], "combined_data")
	# # collect(combined, 1)
	agged = cc.aggregate_count(combined, "heatmap", ["location"], "by_location")
	sorted = cc.sort_by(agged, "sorted", "by_location")
	# multiplied = cc.multiply(sorted, "mult", "by_location", ["by_location", "location"])
	cc.collect(sorted, 1)
	# cc.collect(agged, 1)

	return {in_one, in_two}


if __name__ == "__main__":

	with open(sys.argv[1], "r") as c:
		c = json.load(c)

	# workflow.run(protocol, c, mpc_framework="jiff", apply_optimisations=True)
	workflow.run(protocol, c, mpc_framework="obliv-c", apply_optimisations=True)
