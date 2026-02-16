"""TODO: Add a proper docstring here.
"""

import logging
import ops
import typing

# The unique Charmhub library identifier, never change it
LIBID = "a0959775f225419d86d202cd754066cf"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

HOST_KEY = "host"
PORT_KEY = "port"

logger = logging.getLogger(__name__)


class AirflowAPIServerProvides(ops.Object):
    """A provider handler encapsulating the airflow api server relation."""

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str,
        host: str,
        port: str,
    ):
        super().__init__(charm, relation_name)

        self._charm = charm
        self._relation = charm.model.get_relation(relation_name)

        self._set_api_server_host_info(host, port)

    def _set_api_server_host_info(self, host: str, port: str):
        """Write the API server host and port to the relation."""
        if not self._relation or not self._charm.unit.is_leader():
            return

        if not host:
            logger.error("Invalid host to set in airflow_api_server relation")
            return

        if not port:
            logger.error("Invalid port to set in airflow_api_server relation")
            return

        self._relation.data[self._charm.app][HOST_KEY] = host
        self._relation.data[self._charm.app][PORT_KEY] = port


class AirflowAPIServerRequires(ops.Object):
    """A requirer handler encapsulating the airflow api server relation."""

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str,
        callback: typing.Callable,
    ):
        super().__init__(charm, relation_name)

        self._charm = charm
        self._relation = charm.model.get_relation(relation_name)

        for event in [
            charm.on[relation_name].relation_changed,
            charm.on[relation_name].relation_broken,
        ]:
            self.framework.observe(event, callback)

    @property
    def api_server_host(self) -> typing.Optional[str]:
        """Return API server (host, port) tuple."""
        if not self._relation or not self._relation.app:
            return None

        return self._relation.data[self._relation.app].get(HOST_KEY)

    @property
    def api_server_port(self) -> typing.Optional[str]:
        """Return API server (host, port) tuple."""
        if not self._relation or not self._relation.app:
            return None

        return self._relation.data[self._relation.app].get(PORT_KEY)
