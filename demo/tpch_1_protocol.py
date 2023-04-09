import sys
import json
import conclave.lang as cc
from conclave.utils import *
from conclave import workflow


def protocol():
    cols = "l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment"
    cols = cols.split(",")
    cols_in_one = [defCol(i, "INTEGER", 1) for i in cols]
    # cols_in_one = [
    #     defCol("l_orderkey", "INTEGER", [1]),
    #     defCol("l_partkey", "INTEGER", [1]),
    #     defCol("l_suppkey", "INTEGER", [1]),
    #     defCol("l_linenumber", "INTEGER", [1]),
    #     defCol("l_quantity", "INTEGER", [1]),
    #     defCol("l_extendedprice", "INTEGER", [1]),
    #     defCol("l_discount", "INTEGER", [1]),
    #     defCol("l_tax", "INTEGER", [1]),
    #     defCol("l_returnflag", "INTEGER", [1])
    # ]
    lineitem_one = cc.create("lineitem_cc_one", cols_in_one, {1})
    # lineitem_one = cc.create("lineitem_cc_100_one", cols_in_one, {1})

    # cols_in_two = [
    #     defCol("l_orderkey", "INTEGER", [2]),
    #     defCol("l_partkey", "INTEGER", [2]),
    #     defCol("l_suppkey", "INTEGER", [2]),
    #     defCol("l_linenumber", "INTEGER", [2]),
    #     defCol("l_quantity", "INTEGER", [2]),
    #     defCol("l_extendedprice", "INTEGER", [2]),
    #     defCol("l_discount", "INTEGER", [2]),
    #     defCol("l_tax", "INTEGER", [2]),
    #     defCol("l_returnflag", "INTEGER", [2])
    # ]
    cols_in_two = [defCol(i, "INTEGER", 2) for i in cols]
    
    """
    select
       l_returnflag,
       l_linestatus,
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as sum_base_price,
       sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
       sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
       avg(l_quantity) as avg_qty,
       avg(l_extendedprice) as avg_price,
       avg(l_discount) as avg_disc,
       count(*) as count_order
    from
       lineitem
    where
       l_shipdate <= mdy (12, 01, 1998 ) - 90 units day
    group by
       l_returnflag,
       l_linestatus
    order by
       l_returnflag,
       l_linestatus;
"""
    
    print(cols_in_two)
    

    lineitem_two = cc.create("lineitem_cc_two", cols_in_two, {2})
    # lineitem_two = cc.create("lineitem_cc_100_two", cols_in_two, {2})

    lineitem = cc.concat([lineitem_one, lineitem_two], "lineitem")

    """
    SELECT * FROM lineitem WHERE l_quantity == 50
    """
    # filtered = cc.cc_filter(lineitem, "filtered", "l_quantity", "<", None, 50)
    
    """
    SELECT COUNT(*) AS by_quantity FROM lineitem GROUPBY l_quantity
    """
    agged = cc.aggregate(lineitem, "agged", ["l_returnflag"])
    agged = cc.aggregate_count(lineitem, "heatmap", ["l_quantity"], "by_quantity")

    cc.collect(agged, 1)

    return {lineitem_one, lineitem_two}


if __name__ == "__main__":

	with open(sys.argv[1], "r") as c:
		c = json.load(c)

	# workflow.run(protocol, c, mpc_framework="jiff", apply_optimisations=True)
	workflow.run(protocol, c, mpc_framework="obliv-c", apply_optimisations=True)
