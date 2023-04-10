import contextlib

from distributed import Client, LocalCluster, performance_report


class dask_session:
    def __init__(
        self,
        *,
        memory_limit: str | None = None,
        n_cpus: int | None = None,
        dask_report_filename: str | None = None
    ):
        if memory_limit is None:
            self.memory_limit = "auto"
        else:
            self.memory_limit = memory_limit

        self.n_cpus = n_cpus
        self.filename = dask_report_filename

    def __enter__(self):
        if self.n_cpus is None:
            self.client = Client(
                LocalCluster(memory_limit=self.memory_limit, processes=False)
            )
        else:
            self.client = Client(
                LocalCluster(
                    memory_limit=self.memory_limit,
                    processes=False,
                    n_workers=1,
                    threads_per_worker=self.n_cpus,
                )
            )

        if self.filename is None:
            self.report = contextlib.nullcontext()
        else:
            self.report = performance_report(filename=self.filename)

        self.client.__enter__()
        self.report.__enter__()

        return self

    def __exit__(self, *args):
        """
        making the report requires an intact client, so we exit the report before
        exiting the client
        """
        try:
            self.report.__exit__(*args)
        finally:
            self.client.__exit__(*args)
