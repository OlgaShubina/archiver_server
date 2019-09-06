from flask import Flask, request, abort
import simplejson as json
from collections import defaultdict
import datetime
from table import Archive
from channels import  Channels_
from Query import Query
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import redis_storage
from parsconfig import Config
from flask_cors import CORS, cross_origin
from flask import render_template
from flask import Response

config = Config('/home/olga/PycharmProjects/rest_api/config.yaml')
url = 'postgresql://{}:{}@{}:{}/{}'.format(config.archive["database"]["user"],
                                           config.archive["database"]["password"],
                                           config.archive["database"]["host"],
                                           config.archive["database"]["port"],
                                           config.archive["database"]["dbname"])

engine = sqlalchemy.create_engine(url, client_encoding='utf8')
Session = sessionmaker(bind=engine)

url_compress = 'postgresql://{}:{}@{}:{}/{}'.format(config.compress_archive["database"]["user"],
                                                    config.compress_archive["database"]["password"],
                                                    config.compress_archive["database"]["host"],
                                                    config.compress_archive["database"]["port"],
                                                    config.compress_archive["database"]["dbname"])
engine_compress = sqlalchemy.create_engine(url_compress, client_encoding='utf8')
Session_compress = sessionmaker(bind=engine_compress)


data_base = Archive(engine, Session, Session_compress, config)

app = Flask(__name__)
CORS(app)
app.debug = True
app.logger.debug('Значение для отладки')


@app.route('/template', methods = ["GET", "POST"])
def upload_template():  
    if request.method == 'POST':
        request_data = request.get_json()
        t1 = request_data['t1']
        t2 = request_data['t2']
        channels = request_data['channels']
        level=request_data['level']
        template = render_template("{}".format(config.main["template"]), t1=t1, t2=t2, channels=channels, level=level, path=request_data['path'], host=request_data['host'], port=request_data['port'])
        return Response(template, mimetype=config.main["mimetype"])

@app.route("/channels")
def channels():
    channels = Channels_()
    table_channels = channels.get_channels(data_base.session)
    d = []
    for i in table_channels:
        d.append({"name" : str(i.name), "id" : i.id, "type" : str(i.log_type),
            "units": str(i.units), "threshold" : str(i.threshold), "is_log": str(i.is_log),
            "description" : str(i.description), "cas_type": str(i.cas_type)})
    x = channels.return_tree(d)
    js = json.dumps(x)
    return js


@app.route("/cursor_compress", methods=["GET", "POST"])
def create_chunk_compress():
    if request.method == 'POST':
        try:
            request_data = request.get_json()
            app.logger.debug(request_data)
            if request_data["t1"] > request_data["t2"]:
                app.logger.error("data")
                abort(400)
            query = Query(config.main["query"], request_data["t1"], request_data["t2"], 150.0)
            chunk_list = query.separate(request_data["channels"])
            chunk_dict = {"len_dict": len(chunk_list),
                          "first": {"t1": chunk_list[0].t1,
                                    "t2": chunk_list[0].t2,
                                    "channels": chunk_list[0].ch,
                                    "uuid": chunk_list[0].uuid}}

            js = json.dumps(chunk_dict)
            return js
        except (TypeError, ValueError) as ex:
            app.logger.error(str(ex))
            abort(400)

    else:
        abort(400)

@app.route("/chunk_compress/<chunk_id>", methods=["GET", "POST"])
def chunk_compress(chunk_id):
    if request.method == 'POST':
        try:
            request_data = request.get_json()
            storage = redis_storage.RedisStorage(config.storage["host"], config.storage["port"],
                                                 config.storage["chunk_db"])
            current_chunk = storage.get_chunk(chunk_id)
            if current_chunk is None:
                abort(404)
            data = data_base.get_data(datetime.datetime.fromtimestamp(current_chunk["t1"]),
                                      datetime.datetime.fromtimestamp(current_chunk["t2"]),
                                      current_chunk["channels"], request_data["level"], request_data["fields"])
            if data is None:
                abort(404)
            if current_chunk["next"]:
                data.update({"next": current_chunk["next"]})
            js = json.dumps(data)
            app.logger.debug(current_chunk)
            return js
        except (ValueError) as ex:

            app.logger.error(str(ex))
            abort(400)
    else:
        abort(400)


if __name__ =='__main__':
    app.run()

  
