from __future__ import annotations
from typing import Optional
from datetime import date

from allocation.domain import model
from allocation.domain.model import OrderLine
from allocation.service_layer import unit_of_work


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def add_batch(
    ref: str,
    sku: str,
    qty: int,
    eta: Optional[date],
    uow: unit_of_work.AbstractUnitOfWork,
):
    with uow:
        product = uow.products.get(sku=sku)

        # todo: what happens if batch on a product is added twice?
        if product is None:
            product = model.Product(sku, batches=[model.Batch(ref, sku, qty, eta)])
            uow.products.add(product)
        else:
            product.batches.append(model.Batch(ref, sku, qty, eta))

        uow.commit()


def allocate(
    orderid: str,
    sku: str,
    qty: int,
    uow: unit_of_work.AbstractUnitOfWork,
) -> str:
    line = OrderLine(orderid, sku, qty)

    with uow:
        product = uow.products.get(sku=sku)
        if product is None or not is_valid_sku(sku, product.batches):
            raise InvalidSku(f"Invalid sku {line.sku}")

        batchref = product.allocate(line)
        uow.commit()
    return batchref
