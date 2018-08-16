from flask import Flask
from futu import futu_sync
from ib import ib_sync
from td import td_sync

app = Flask(__name__)
app.register_blueprint(futu_sync.futu_sync_app)
app.register_blueprint(ib_sync.ib_sync_app)
app.register_blueprint(td_sync.td_sync_app)
