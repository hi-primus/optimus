from distributed import Client

from optimus.engines.base.dask.constants import STARTING_DASK
from optimus.helpers.constants import JUST_CHECKING
from optimus.helpers.logger import logger


class Dask:
    def __init__(self):
        self._dask = None
        self._client = None

    def create(self, *args, **kwargs):
        logger.print(JUST_CHECKING)
        logger.print("-----")

        logger.print(STARTING_DASK)

        # Create Dask client
        self._client = Client(*args, **kwargs)

        # Print cluster info
        # self._client.scheduler_info()["workers]

        return self

    def load(self, session):
        self._client = session
        return self

    @property
    def dask(self):
        return self._client

    def compute(self, *args, **kwargs):
        return self._client.compute(*args, **kwargs)
