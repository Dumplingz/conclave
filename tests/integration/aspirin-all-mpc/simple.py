import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    pid_col_meds = "0"
    med_col_meds = "4"
    date_col_meds = "7"

    pid_col_diags = "8"
    diag_col_diags = "16"
    date_col_diags = "18"

    num_med_cols = 8 
    medication_cols = [defCol(str(i), "INTEGER", [1]) for i in range(num_med_cols)]

    medication = cc.create("medication", medication_cols, {1})

    num_diag_cols = 13 
    diagnosis_cols = [defCol(str(i + num_med_cols), "INTEGER", [1]) for i in range(num_diag_cols)]

    diagnosis = cc.create("diagnosis", diagnosis_cols, {1})

    # only keep relevant columns
    medication_proj = cc.project(medication, "medication_proj", [pid_col_meds, med_col_meds, date_col_meds])
    diagnosis_proj = cc.project(diagnosis, "diagnosis_proj", [pid_col_diags, diag_col_diags, date_col_diags])
    
    aspirin = cc.cc_filter(medication_proj, "aspirin", med_col_meds, "==", scalar=1)
    heart_patients = cc.cc_filter(diagnosis_proj, "heart_patients", diag_col_diags, "==", scalar=1)

    joined = cc.join(aspirin, heart_patients, "joined", [pid_col_meds], [pid_col_diags])
    cases = cc.cc_filter(joined, "cases", date_col_diags, "<", other_col_name=date_col_meds)

    cc.distinct_count(cases, "expected", pid_col_meds)

    return {medication, diagnosis}


if __name__ == "__main__":
    pid = sys.argv[1]
    # define name for the workflow
    workflow_name = "aspirin-real-test-" + pid
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.all_pids = [1]
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=False, use_hdfs=False)
    conclave_config.with_sharemind_config(sharemind_conf)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # point conclave to the directory where the generated code should be stored/ read from
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    # point conclave to directory where data is to be read from...
    conclave_config.input_path = os.path.join(current_dir, "data")
    # and written to
    conclave_config.output_path = os.path.join(current_dir, "data")
    # define this party's unique ID (in this demo there is only one party)
    job_queue = generate_code(protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=False)
    dispatch_jobs(job_queue, conclave_config)