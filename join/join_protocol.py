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
    orders_one = cc.create("orders1", cols_in_one, {1})
    cols_in_two = [defCol(i, "INTEGER", 2) for i in orders_cols]
    orders_two = cc.create("orders2", cols_in_two, {2})

    """Lineitem"""
    lineitem_cols = "l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment"
    lineitem_cols = lineitem_cols.split(",")
    cols_in_one = [defCol(i, "INTEGER", 1) for i in lineitem_cols]
    lineitem_one = cc.create("lineitem0", cols_in_one, {1})
    cols_in_two = [defCol(i, "INTEGER", 2) for i in lineitem_cols]
    lineitem_two = cc.create("lineitem1", cols_in_two, {2})
    
    """SELECT o_orderpriority, COUNT(*) AS order_count
  FROM orders
 WHERE o_orderdate >= MDY (7, 1, 1993)
   AND o_orderdate < MDY (7, 1, 1993) + 3 UNITS MONTH
   AND EXISTS (
      SELECT *
        FROM lineitem
       WHERE l_orderkey = o_orderkey
         AND l_commitdate < l_receiptdate
     )
GROUP BY o_orderpriority
ORDER BY o_orderpriority

    """

    # lineitem_two = cc.create("lineitem_cc_100_two", cols_in_two, {2})
    
        
    # customer = cc.concat([customer_one, customer_two], "customer")
    # orders = cc.concat([orders_one, orders_two], "orders")
    # lineitem = cc.concat([lineitem_one, lineitem_two], "lineitem")
    # supplier = cc.concat([], "supplier")

    """SELECT * FROM orders_one, orders_two WHERE o_custkey = o_custkey, o_orderpriority == 3"""
    
    # Steven: select count (*) from o1, o2 where o1.key = o2.key group by o_orderpriority

    distinct_one = cc.distinct(orders_one, "distinct_one", ["o_custkey"])
    distinct_two = cc.distinct(orders_two, "distinct_two", ["o_custkey"])

    custkey = cc.join(distinct_one, distinct_two, "custkey", ["o_custkey"], ["o_custkey"])

    # partkey = cc.join(lineitem_one, lineitem_two, "partkey", ["l_partkey"], ["l_partkey"])
    # agged = cc.aggregate_count(custkey, "count", ["o_orderpriority"], "count")
    # filtered = cc.cc_filter(custkey, "filtered", "o_orderpriority", "==", None, 0)
    
    # sorted = cc.sort_by(agged, "sorted", "o_orderpriority")
    cc.collect(custkey, 1)

    return {orders_one, orders_two}

    # return {lineitem_one, lineitem_two}

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
    # input_size = "1MB"
    # num_trials = 1

    input_size = sys.argv[1]
    num_trials = int(sys.argv[2])
    

    party_one_config = "tpch_config_one.json"
    party_two_config = "tpch_config_two.json"


    out_file = input_size + "join.csv"

    data_path_one = "/home/chz_uchicago_edu/data-pooling-experiments/conclave/join/tpch_one/" + input_size
    data_path_two = "/home/chz_uchicago_edu/data-pooling-experiments/conclave/join/tpch_two/" + input_size

    for i in range(num_trials):

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
