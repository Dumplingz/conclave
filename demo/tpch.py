import time
import json
import sys
from multiprocessing import Process
from conclave import workflow

import tpch_1_protocol as t1p
import tpch_4_protocol as t4p
import tpch_5_protocol as t5p
import tpch_6_protocol as t6p


def party_thread(config_path, protocol, data_path):
    """
    Thread for party two
    """
    with open(config_path, "r") as c:
        c = json.load(c)
    c['user_config']['paths']['input_path'] = data_path

    workflow.run(protocol, c, mpc_framework="obliv-c", apply_optimisations=True)
    return

if __name__ == "__main__":
    num_trials = 10
    permitted_file_sizes = ["1MB", "10MB", "100MB", "1GB"]
    permitted_tpch_queries = {"1":t1p.protocol,"4":t4p.protocol,"5":t5p.protocol,"6":t6p.protocol}

    party_one_config = "tpch_config_one.json"
    party_two_config = "tpch_config_two.json"
    
    input_size = sys.argv[1]
    if input_size not in permitted_file_sizes:
        raise Exception("Not supported input size")

    tpch_num = sys.argv[2]
    if tpch_num not in list(permitted_tpch_queries.keys()):
        raise Exception("Not supported tpch query number")

    data_path_one = "/home/cc/conclave/demo/tpch_one/" + input_size
    data_path_two = "/home/cc/conclave/demo/tpch_two/" + input_size

    out_file = input_size + "_tpch_{}_out.txt".format(tpch_num)
    protocol = permitted_tpch_queries[tpch_num]

    for i in range(num_trials):
        # start party 2 process
        party_two_process = Process(target=party_thread, args=[party_two_config, protocol, data_path_two])
        party_two_process.start()

        start = time.perf_counter()
        with open(party_one_config, "r") as c:
            c = json.load(c)
        c['user_config']['paths']['input_path'] = data_path_one
        
        print(c)
        # workflow.run(protocol, c, mpc_framework="jiff", apply_optimisations=True)
        
        # start party 1
        workflow.run(protocol, c, mpc_framework="obliv-c", apply_optimisations=True)

        # wait until party 2 process finishes
        party_two_process.join()
        end = time.perf_counter()
        time_taken = end-start
        print("took {} seconds to run MPC".format(time_taken))

        out_line = "{},{},{}\n".format(input_size, time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), time_taken)
        with open(out_file, "a+") as myfile:
            myfile.write(out_line)
