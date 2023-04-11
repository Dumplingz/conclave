import sys
import json
import time
from multiprocessing import Process
import conclave.lang as cc
from conclave.utils import *
from conclave import workflow


def protocol():

    """Customer"""
    customer_cols = "c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment"
    customer_cols = customer_cols.split(",")
    cols_in_one = [defCol(i, "INTEGER", 1) for i in customer_cols]
    customer_one = cc.create("customer0", cols_in_one, {1})
    cols_in_two = [defCol(i, "INTEGER", 2) for i in customer_cols]
    customer_two = cc.create("customer1", cols_in_two, {2})
    
    """Orders"""
    orders_cols = "o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment"
    orders_cols = orders_cols.split(",")
    cols_in_one = [defCol(i, "INTEGER", 1) for i in orders_cols]
    orders_one = cc.create("orders0", cols_in_one, {1})
    cols_in_two = [defCol(i, "INTEGER", 2) for i in orders_cols]
    orders_two = cc.create("orders1", cols_in_two, {2})

    """Lineitem"""
    lineitem_cols = "l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment"
    lineitem_cols = lineitem_cols.split(",")
    cols_in_one = [defCol(i, "INTEGER", 1) for i in lineitem_cols]
    lineitem_one = cc.create("lineitem0", cols_in_one, {1})
    cols_in_two = [defCol(i, "INTEGER", 2) for i in lineitem_cols]
    lineitem_two = cc.create("lineitem1", cols_in_two, {2})
    
    lineitem = cc.concat([lineitem_one, lineitem_two], "lineitem")

    """SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= MDY(1,1,1994)
    AND l_shipdate < MDY(1,1,1994) + 1 UNITS YEAR
    AND l_discount BETWEEN .06 - 0.01 AND .06 + 0.010001
    AND l_quantity < 24
"""

    """SELECT (l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_quantity == 24"""

    mult = cc.multiply(lineitem, "mult", "revenue", ["l_extendedprice", "l_discount"])
    filtered = cc.cc_filter(mult, "filtered", "l_quantity", "==", None, 24)
    cc.collect(filtered, 1)

    return {lineitem_one, lineitem_two}


def party_two_thread(config_path, protocol, data_path):
    """
    Thread for party two
    """
    with open(config_path, "r") as c:
        c = json.load(c)
    c['user_config']['paths']['input_path'] = data_path

    workflow.run(protocol, c, mpc_framework="obliv-c", apply_optimisations=True)
    return

if __name__ == "__main__":

    party_one_config = "tpch_config_one.json"
    party_two_config = "tpch_config_two.json"

    input_size = "1MB"

    out_file = input_size + "_tpch_6_out.txt"

    data_path_one = "/home/cc/conclave/demo/tpch_one/" + input_size
    data_path_two = "/home/cc/conclave/demo/tpch_two/" + input_size

    for i in range(10):

        party_two_process = Process(target=party_two_thread, args=[party_two_config, protocol, data_path_two])
        party_two_process.start()

        start = time.perf_counter()
        with open(party_one_config, "r") as c:
            c = json.load(c)
        c['user_config']['paths']['input_path'] = data_path_one
        
        print(c)
        # workflow.run(protocol, c, mpc_framework="jiff", apply_optimisations=True)
        workflow.run(protocol, c, mpc_framework="obliv-c", apply_optimisations=True)
        end = time.perf_counter()
        time_taken = end-start
        print("took {} seconds to run MPC".format(time_taken))

        out_line = "{},{},{}\n".format(input_size, time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), time_taken)
        with open(out_file, "a+") as myfile:
            myfile.write(out_line)
