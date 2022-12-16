import cloudpickle

from parallelManager.backends.dask.dask_utils import init_dask
import concurrent
from concurrent.futures import Executor
import logging as python_logging
from typing import Literal
from tqdm import tqdm
from parallelManager.backends import defaults
from parallelManager.executors import DummyExecutor

#TODO: Create a classmethod or a function that creates JobLaunchers and keeps track of the resourecs allocated

class JobLauncher():
    WAIT_TIMEOUT = 360000

    def __init__(self, n_workers:int=1, n_cores:int=1, level:Literal["nodes", "processes","threads", "debug"]="processes",
                 dask_cluster:str="condor", monitoring=True, batch_factor=4,
                 verbose:bool=False, **kwargs):
        """

        :param n_workers: Number of workers to use
        :param n_cores: Number of workers to use
        :param level: where to submit the task. To a node, a process o a thread
        :param dask_cluster: For level=="nodes", which jobque to use
        :param monitoring: Activate dask dashboard
        :param batch_factor: The batch size for executing map functions is computed as n_workers*batch_factor
        :param verbose: Print log information
        :param kwargs:
        """

        self.n_workers = n_workers or getattr(defaults, "n_workers", 1)
        self.n_cores =  n_cores or getattr(defaults, "n_cores", 1)
        self.level = level or getattr(defaults, "level", None)
        self.dask_cluster = dask_cluster or getattr(defaults, "dask_cluster", None)
        self.batch_factor = batch_factor
        self.verbose = verbose
        self.kwargs = kwargs or getattr(defaults, "kwargs", None)
        if not monitoring:
            self.kwargs["dashboard_address"] = None
        assert self.level in ["nodes", "processes", "threads", "debug"]

        if self.level == "debug":
            self.istarmap = lambda fun, args_list, batch_factor=self.batch_factor: (fun(*args) for args in args_list)
            self.starmap = lambda fun, args_list, batch_factor=self.batch_factor: [fun(*args) for args in args_list]
        else:
            self.istarmap = self._batched_istarmap
            self.starmap = self._batched_starmap

        #Delayed attributes
        self._executor = None

    def _get_executor(self):
        if self._executor is None:
            if self.level == "nodes":
                self._executor = init_dask(dask_cluster=self.dask_cluster, n_nodes=self.n_workers, n_cores=self.n_cores, **self.kwargs)
            elif self.level == "processes":
                import multiprocessing
                ctx = multiprocessing.get_context('spawn')
                self._executor = concurrent.futures.ProcessPoolExecutor(self.n_workers, mp_context=ctx)
                _old_submit = self._executor.submit

                def robust_submit(fun, /,*args, **kwargs):
                    bytes_ = cloudpickle.dumps((fun,args, kwargs))
                    return _old_submit(deserialize_and_execute, bytes_)
                self._executor.submit = robust_submit

            elif self.level == "daskprocesses":
                self._executor = init_dask(dask_cluster="local", n_dask_workers=self.n_workers, **self.kwargs)
            elif self.level == "threads":
                self._executor = concurrent.futures.ThreadPoolExecutor(self.n_workers)
            elif self.level =="debug":
                self._executor = DummyExecutor()
            else:
                raise Exception(f"Error, level ({self.level}) is not valid")
        else:
            print("Executor already exists")
        return self._executor

    def close(self):
        if self.level in ["nodes", "daskprocesses"]:
            self._executor.shutdown() #close()
        elif self._executor:
            self._executor.shutdown()

    def __del__(self):
        self.close()


    def wait_future(self, *args, **kwargs):
        if self.level in ["nodes", "daskprocesses"]:
            from dask.distributed import wait as wait_dask
            from dask.distributed.client import FIRST_COMPLETED
            return wait_dask( *args, **kwargs, return_when=FIRST_COMPLETED)
        else:
            from concurrent.futures import wait as wait_concurrent, FIRST_COMPLETED
            return wait_concurrent( *args, **kwargs, return_when=FIRST_COMPLETED)

    def as_completed(self, *args, **kwargs):
        if self.level in ["nodes", "daskprocesses"]:
            from dask.distributed import as_completed as as_completed_dask
            return as_completed_dask(*args, **kwargs)
        else:
            from concurrent.futures import as_completed as as_completed_concurrent
            return as_completed_concurrent(*args, **kwargs)

    def get_logger(self):
        if self.level in ["daskprocesses", "nodes"]:
            from distributed.worker import logger
            return logger
        else:
            return python_logging

    def _batched_istarmap(self, fun, args_list, batch_factor=None):
        executor = self._get_executor()
        futures_dict = {}
        n_submitted= 0
        batch_factor = batch_factor if batch_factor is not None else self.batch_factor
        max_to_run = self.n_workers*batch_factor
        for nArg, args in tqdm(enumerate(args_list), disable=not self.verbose):
            if n_submitted > max_to_run:
                self.wait_future(list(futures_dict.values()), timeout=self.WAIT_TIMEOUT)
                for i in list(futures_dict.keys()):
                    fut = futures_dict[i]
                    if fut.done():
                        n_submitted -= 1
                        result = fut.result()
                        if hasattr(fut, "release"):
                            fut.release()
                        del futures_dict[i]
                        yield result

            fut = executor.submit(fun, *args)
            futures_dict[nArg] = fut
            n_submitted += 1
        for fut in self.as_completed(list(futures_dict.values())):
                result = fut.result()
                if hasattr(fut, "release"):
                    fut.release()
                yield result
        del futures_dict

    def _batched_starmap(self, fun, args_list, batch_factor=None):
        batch_factor = batch_factor if batch_factor is not None else self.batch_factor
        return [ x for x in tqdm(self.istarmap(fun, list(args_list), batch_factor), disable=not self.verbose) ]

    def map(self, fun, args_list):
        results = self._get_executor().map(fun, list(args_list))
        if self.level in ["nodes", "daskprocesses"]:
            results = self._get_executor().gather(results)
        return results

    #TODO implement __enter__ __exit to use it within context manager


def deserialize_and_execute(bytes_):
    func, args, kwargs = cloudpickle.loads(bytes_)
    return func(*args, **kwargs)