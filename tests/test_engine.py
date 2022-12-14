import time
from unittest import TestCase

from parallelManager.engine import JobLauncher


class TestJobLauncher(TestCase):
    def test_process(self):
        t0 = time.time()
        engine = JobLauncher(n_workers=4, level="processes", verbose=True)
        try:
            logger = engine.get_logger()

            def computation(i):
                logger.warning(f"Computing {i}")
                import numpy as np
                def fun(x):  # This is a stupid function to make the calculations slow
                    for k2 in range(1, x + i):
                        np.sqrt(np.random.choice(np.random.rand(x)) ** 2)

                list(map(fun, range(1, 1000)))
                return i

            results = engine.istarmap(computation, ((x,) for x in range(10)))
            results = sorted(results)

            self.assertTrue(results == sorted(range(10)))
            t = time.time() - t0
            # print("Time=", t)
            self.assertLess(t, 40)
        finally:
            engine.close()

    def test_process_threadPool(self):

        engine = JobLauncher(n_workers=4, level="processes", verbose=True)
        try:
            t0 = time.time()
            logger = engine.get_logger()

            def computation(i):
                logger.warning(f"Computing {i}")
                import numpy as np
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(2) as pool:
                    def fun(x):
                        for k2 in range(1, x + i):
                            np.sqrt(np.random.choice(np.random.rand(x)) ** 2)

                    list(pool.map(fun, range(1, 1000)))
                return i

            results = engine.istarmap(computation, ((x,) for x in range(10)))
            results = sorted(results)

            self.assertTrue(results == sorted(range(10)))
            t = time.time() - t0
            # print("Time=", t)
            self.assertLess(t, 60)
        finally:
            engine.close()

    def test_process_ProccessPool(self):

        engine = JobLauncher(n_workers=3, level="processes", verbose=True)
        try:
            t0 = time.time()
            logger = engine.get_logger()

            def computation(i):
                inner_engine = JobLauncher(n_workers=2, level="processes", verbose=False)
                try:
                    logger.warning(f"Computing {i}")
                    import numpy as np
                    def fun(x):
                        for k2 in range(1, i + x):
                            np.sqrt(np.random.choice(np.random.rand(x)) ** 2).sum()
                        return i
                        # time.sleep(0.0005)

                    results = list(inner_engine.istarmap(fun, ((x,) for x in range(1, 1000))))
                    # list(inner_engine.map(fun, range(1,10000)))
                    inner_engine.close()
                    logger.warning(f"Done {i}. Total sum= {sum(results)}")
                finally:
                    inner_engine.close()
                return i

            results = engine.istarmap(computation, ((x,) for x in range(10)))
            results = sorted(results)
            self.assertTrue(results == sorted(range(10)))
            t = time.time() - t0
            # print("Time=", t)
            self.assertLess(t, 60)
        finally:
            engine.close()

    def test_two_maps(self):
        from concurrent.futures import ProcessPoolExecutor


        t0 = time.time()
        engine = JobLauncher(n_workers=2, level="processes", verbose=True)
        try:
            def computation(i):
                return i ** 2

            results = engine.istarmap(computation, ((i,) for i in range(10)))
            # print(list(results))
            # print("Time=", time.time() - t0)

            results = engine.map(computation, range(10))
            self.assertTrue(list(map(computation, range(10))) == list(results))
            # print(list(results))
            # print("Time=", time.time() - t0)
            engine.close()
        finally:
            engine.close()