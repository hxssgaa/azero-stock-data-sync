import logging
from ib import ib_sync
from td import td_sync
from flask import Flask
from utils import get_config
from common.utils import DbCache

app = Flask(__name__)
app.register_blueprint(ib_sync.ib_sync_app)
app.register_blueprint(td_sync.td_sync_app)
DbCache('azero').clear()
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    cfg = get_config('server')
    app.run(threaded=True, debug=True, host=cfg['ServerHost'], port=int(
        cfg['ServerPort']))
