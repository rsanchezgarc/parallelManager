from concurrent.futures import Executor, Future


class DummyExecutor(Executor):

    def __init__(self):
        self._shutdown = False

    def submit(self, fn, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError('cannot schedule new futures after shutdown')

        f = Future()
        try:
            result = fn(*args, **kwargs)
        except BaseException as e:
            f.set_exception(e)
        else:
            f.set_result(result)

        return f

    def shutdown(self, wait=True, **kwargs):
        self._shutdown = True
