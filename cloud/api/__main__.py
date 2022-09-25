from aiohttp import web

from cloud.api.handlers import ImportsView, NodesView
from cloud.utils.db import pg_context

# todo:
#  -write configs
#  -validation
#  -drop enum type in alembic config
#  -think about to change ids from string to int. does is matter?
#  -remove sec fractions from db
#  -00:00 to Z in GET/nodes/id response json

def main():
    app = web.Application()
    app.cleanup_ctx.append(pg_context)

    app.router.add_route('POST', ImportsView.URL_PATH, ImportsView)
    app.router.add_route('GET', NodesView.URL_PATH, NodesView)
    web.run_app(app, host='127.0.0.1', port=8080)


if __name__ == '__main__':
    main()
