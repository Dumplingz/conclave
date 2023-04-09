import sys
import json
import time
import conclave.lang as cc
from conclave.utils import *
from conclave import workflow


def protocol():

    cols_in_one = [
        defCol("car_id", "INTEGER", [1]),
        defCol("location", "INTEGER", [1]),
        defCol("one", "INTEGER", [1])
    ]
    lineitem_one = cc.create("in1", cols_in_one, {1})

    cols_in_two = [
        defCol("car_id", "INTEGER", [2]),
        defCol("location", "INTEGER", [2]),
        defCol("one", "INTEGER", [2])
    ]
    lineitem_two = cc.create("in2", cols_in_two, {2})

    lineitem = cc.concat([lineitem_one, lineitem_two], "lineitem")

    # mult = cc.multiply(lineitem, "mult", "revenue", ["l_extendedprice", "l_discount"])
    mult = cc.multiply(lineitem, "mult", "location", ["car_id", "location"])

    """SELECT SUM(l_extendedprice * l_discount) FROM lineitem"""
    # summed = cc.aggregate(mult, "summed", [], "revenue", "sum")
    summed = cc.aggregate(mult, "summed", ["one"], "location", "sum", "summed_col")
    cc.collect(summed, 1)

    return {lineitem_one, lineitem_two}


if __name__ == "__main__":

    start = time.perf_counter()


    with open(sys.argv[1], "r") as c:
        c = json.load(c)

    # workflow.run(protocol, c, mpc_framework="jiff", apply_optimisations=True)
    workflow.run(protocol, c, mpc_framework="obliv-c", apply_optimisations=True)
    end = time.perf_counter()
    print("took {} seconds to run MPC".format(end-start))