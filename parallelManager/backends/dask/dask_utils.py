import warnings
from datetime import datetime
import parallelManager.backends.dask.defaults as defaults

def init_dask(dask_cluster = "condor", cores=1, jobs=1, memory="8GB", disk="4GB", n_gpus=None,
              dask_config_vars=defaults.DASK_DEFAULT_CONFIG, worker_init_funtions=defaults.WORKER_INIT_FUNCTIONS,
              dashboard_address=':0'):

    from dask_jobqueue import HTCondorCluster
    from dask.distributed import Client
    import dask
    with dask.config.set(dask_config_vars):

        serializers = None #['custom_torch_serizalizer', "dask", "pickle"] # None

        for fun in worker_init_funtions:
            fun()

        if n_gpus:
            submit_command_extra = ["request_gpus=%d"%n_gpus]
        else:
            submit_command_extra = None

        if dask_cluster=="condor":
            cluster = HTCondorCluster(cores=cores, memory=memory, disk=disk, submit_command_extra=submit_command_extra,
                                      log_directory=f"/tmp/HTCondorCluster_{datetime.now().strftime('%m-%d-%Y_%H-%M-%S')}/",
                                      dashboard_address=dashboard_address)
            print("dashboard_link", cluster.dashboard_link)
            # cluster.scale(jobs=jobs)  # ask for N jobs
            cluster.adapt(maximum_jobs=jobs) # ask for as much as 10 jobs
            client = Client(cluster)

        elif dask_cluster == "local":
            # from dask_jobqueue.local import LocalCluster
            # cluster = LocalCluster(cores=jobs, memory=memory, processes=True ) #dashboard_address=None
            # print(cluster.dashboard_link)
            # # cluster.scale(jobs=jobs)  # ask for N jobs
            # # cluster.adapt(maximum_jobs=jobs) # ask for as much as 10 jobs
            from distributed import LocalCluster
            cluster = LocalCluster(n_workers=jobs, processes=True, scheduler_port=0, dashboard_address=dashboard_address)
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
