from flask import Flask, render_template
from utils import *
from futu import futu_sync

app = Flask(__name__)
app.register_blueprint(futu_sync)

if __name__ == "__main__":
    cfg = get_config('server')
    app.run(threaded=True, host=cfg['ServerHost'], port=int(cfg['ServerPort']))
