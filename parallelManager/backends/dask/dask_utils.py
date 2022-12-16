import warnings
from datetime import datetime
from typing import Literal

import parallelManager.backends.dask.defaults as defaults

def init_dask(dask_cluster:Literal["condor","local","debug"] = "condor", n_nodes=1, n_dask_workers=1, n_cores=1,
              memory="8GB", disk="4GB", n_gpus=None, log_directory_prefix="/tmp/HTCondorCluster_",
              dask_config_vars=defaults.DASK_DEFAULT_CONFIG, worker_init_funtions=defaults.WORKER_INIT_FUNCTIONS,
              dashboard_address=':0'):

    from dask_jobqueue import HTCondorCluster
    from dask.distributed import Client
    import dask
    with dask.config.set(dask_config_vars):

        serializers = None #['custom_torch_serizalizer', "dask", "pickle"] # None

        for fun in worker_init_funtions:
            fun()

        assert n_dask_workers == 1 or n_cores == 1, "Error, n_dask_workers == 1 or n_cores == 1 required. Decide if you want" \
                                                    "to have one worker with multiple cores of multiple workers with one single core"

        env_extra= [f'export {k.upper().replace(".", "__")}"{v}"' for k,v in dask_config_vars.items()]
        submit_command_extra = ["request_cpus=%d"%n_cores]

        if n_gpus:
            submit_command_extra += ["request_gpus=%d"%n_gpus] # it could be also done with job_extra={"request_gpus":1}

        if dask_cluster=="condor":
            cluster = HTCondorCluster(cores=n_dask_workers, memory=memory, disk=disk, submit_command_extra=submit_command_extra,
                                      env_extra=env_extra,
                                      # job_extra={"request_cpus": n_cpus},  # ,"request_gpus":1} # This can be done with submit_command_extra instead
                                      log_directory=log_directory_prefix + datetime.now().strftime('%m-%d-%Y_%H-%M-%S'),
                                      dashboard_address=dashboard_address)
            print("dashboard_link", cluster.dashboard_link)
            cluster.adapt(maximum_jobs=n_nodes)
            print(cluster.job_script())
            client = Client(cluster)

        elif dask_cluster == "local":
            # from dask_jobqueue.local import LocalCluster
            # cluster = LocalCluster(cores=jobs, memory=memory, processes=True ) #dashboard_address=None
            # print(cluster.dashboard_link)
            # # cluster.scale(jobs=jobs)  # ask for N jobs
            # # cluster.adapt(maximum_jobs=jobs) # ask for as much as 10 jobs
            from distributed import LocalCluster
            cluster = LocalCluster(n_workers=n_nodes, processes=True, scheduler_port=0, dashboard_address=dashboard_address)
            print("dashboard_link", cluster.dashboard_link)
            client = Client(cluster, serializers=serializers)

        elif dask_cluster == "debug":
            client = Client(processes=False, serializers=serializers)
        else:
            raise ValueError(f"non-valid dask_cluster option {dask_cluster}")

        client.run(lambda: warnings.filterwarnings("ignore", category=DeprecationWarning))

        def initFun():
            for fun in worker_init_funtions:
                fun()
        client.run(initFun)
        # print("Dask init")
        return client
