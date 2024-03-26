from datetime import datetime
from flask import Flask, request

from allocation.adapters import orm
from allocation.domain import events
from allocation.service_layer import unit_of_work, messagebus
from allocation.service_layer.handlers import InvalidSku

app = Flask(__name__)
orm.start_mappers()


@app.route("/add_batch", methods=["POST"])
def add_batch():
    eta = request.json["eta"]
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    messagebus.handle(
        events.BatchCreated(
            request.json["ref"], request.json["sku"], request.json["qty"], eta
        ),
        unit_of_work.SqlAlchemyUnitOfWork(),
    )
    return "OK", 201


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    try:
        results = messagebus.handle(
            events.AllocationRequired(
                request.json["orderid"], request.json["sku"], request.json["qty"]
            ),
            unit_of_work.SqlAlchemyUnitOfWork(),
        )
        batchref = results.pop(0)
    except InvalidSku as e:
        return {"message": str(e)}, 400

    return {"batchref": batchref}, 201
