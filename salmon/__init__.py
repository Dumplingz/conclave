import salmon.dag as saldag
import salmon.comp as comp
import salmon.partition as part
from salmon.codegen import CodeGenConfig
from salmon.codegen.sharemind import SharemindCodeGen
from salmon.codegen.spark import SparkCodeGen

def codegen(protocol, config):

    # set up code gen config object
    cfg = CodeGenConfig.from_dict(config)

    # apply optimizations
    dag = comp.rewriteDag(saldag.OpDag(protocol()))
    # prune for party
    pruned = comp.pruneDag(dag, cfg.sharemind_pid)
    # partition into subdags that will run in specific frameworks
    mapping = part.heupart(dag)
    # for each sub dag run code gen and add resulting job to job queue
    jobqueue = []
    for job_num, (fmwk, subdag) in enumerate(mapping):
        print(job_num, fmwk)
        if fmwk == "sharemind":
            job = SharemindCodeGen(cfg, subdag, cfg.sharemind_pid).generate(
                "sharemind-job-" + str(job_num), cfg.output_path)
            jobqueue.append(job)
        elif fmwk == "spark":
            job = SparkCodeGen(cfg, subdag).generate(
                "spark-job-" + str(job_num), cfg.output_path)
            jobqueue.append(job)
        else:
            raise Exception("Unknown framework: " + fmwk)
    # return job
    return jobqueue
