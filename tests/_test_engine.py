import time
from parallelManager.engine import JobLauncher


def test_process():
    t0 = time.time()
    engine = JobLauncher(n_workers=5, level="processes", verbose=True, dask_cluster="condor")
    logger = engine.get_logger()
    def computation(i):
        logger.warning(f"Computing {i}")
        import numpy as np
        def fun(x):
            for k2 in range(1,x+i):
                np.sqrt(np.random.choice(np.random.rand(x)) ** 2)
        list(map(fun, range(1,1000)))
        return i

    results = engine.istarmap(computation, ((x,) for x in range(10)))
    print(list(results))
    print("Time=", time.time()-t0)

def test_process_with_pool():
    import concurrent
    t0 = time.time()
    engine = JobLauncher(n_workers=5, level="processes", verbose=True, dask_cluster="condor")
    logger = engine.get_logger()
    def computation(i):
        logger.warning(f"Computing {i}")
        import numpy as np
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            def fun(x):
                for k2 in range(1,x+i):
                    np.sqrt(np.random.choice(np.random.rand(x)) ** 2)
            list(pool.map(fun, range(1,1000)))
        return i

    results = engine.istarmap(computation, ((x,) for x in range(10)))
    print(list(results))
    print("Time=", time.time()-t0)


def test_process_within_process():
    t0 = time.time()
    engine = JobLauncher(n_workers=5, level="processes", verbose=True)
    logger = engine.get_logger()
    def computation(i):
        inner_engine = JobLauncher(n_workers=2, level="processes", verbose=False)
        logger.warning(f"Computing {i}")
        import numpy as np
        def fun(x):
            for k2 in range(1, i+x):
                return np.sqrt(np.random.choice(np.random.rand(x)) ** 2).sum()
            return 0
            # time.sleep(0.0005)
        results = list(inner_engine.istarmap(fun,  ((x,) for x in range(1,10000))))
        # list(inner_engine.map(fun, range(1,10000)))
        inner_engine.close()
        results = np.sum(results)
        logger.warning(f"Done {i}. Total sum= {results}")
        return results

    results = engine.istarmap(computation, ((i,) for i in range(10)))
    # results = engine.map(computation, range(10))

    print(list(results))
    print("Time=", time.time()-t0)
    engine.close()
    # time.sleep(10)

if __name__ == "__main__":
    test_process_within_process()
    print("main finished!!")