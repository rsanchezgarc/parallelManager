n_workers=1
level = "processess",
dask_cluster = "condor",
kwargs = dict(cores=1, n_gpus=1, disk="8GB", memory="32GB")
DASK_DEFAULT_CONFIG={
    "distributed.worker.daemon":False, #" #Daemonic process cannot have childrends, so set it to False for nested processes
    "distributed.comm.timeouts.connect": 100,
}
#    "export CUPY_GPU_MEMORY_LIMIT": "90%"
WORKER_INIT_FUNCTIONS=[] #[patch_tensor_serialization]