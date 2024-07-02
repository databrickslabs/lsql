import contextlib
import functools
import typing
from http.server import BaseHTTPRequestHandler

from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.__about__ import __version__
from databricks.labs.lsql.dashboards import Dashboards


@contextlib.contextmanager
def http_fixture_server(handler: typing.Callable[[BaseHTTPRequestHandler], None]):
    from http.server import HTTPServer
    from threading import Thread

    class _handler(BaseHTTPRequestHandler):
        def __init__(self, handler: typing.Callable[[BaseHTTPRequestHandler], None], *args):
            self._handler = handler
            super().__init__(*args)

        def __getattr__(self, item):
            if "do_" != item[0:3]:
                raise AttributeError(f"method {item} not found")
            return functools.partial(self._handler, self)

    handler_factory = functools.partial(_handler, handler)
    srv = HTTPServer(("localhost", 0), handler_factory)
    t = Thread(target=srv.serve_forever)
    try:
        t.daemon = True
        t.start()
        yield "http://{0}:{1}".format(*srv.server_address)
    finally:
        srv.shutdown()


def test_user_agent_is_propagated():
    user_agent = {}

    def inner(h: BaseHTTPRequestHandler):
        for pair in h.headers["User-Agent"].split(" "):
            if "/" not in pair:
                continue
            k, v = pair.split("/")
            user_agent[k] = v
        h.send_response(200)
        h.send_header("Content-Type", "application/json")
        h.end_headers()
        h.wfile.write(b"{}")
        h.wfile.flush()

    with http_fixture_server(inner) as host:
        ws = WorkspaceClient(host=host, token="_")
        d = Dashboards(ws)
        d.get_dashboard("...")

    assert "lsql" in user_agent
    assert user_agent["lsql"] == __version__
