import abc
import model
from sqlalchemy import text


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        select_batch_id = "SELECT id from batches WHERE reference=:reference"
        existing_batch = self.session.execute(select_batch_id, dict(reference=batch.reference)).fetchone()
        batch_id = None

        if existing_batch:
            batch_id = existing_batch[0]
        else:
            insert_batch = "INSERT INTO batches (reference, sku, _purchased_quantity, eta) VALUES (:reference, :sku, :_purchased_quantity, :eta)"
            self.session.execute(insert_batch, dict(reference=batch.reference, sku=batch.sku, _purchased_quantity=batch._purchased_quantity,
                                                    eta=batch.eta))
            result = self.session.execute(select_batch_id, dict(reference=batch.reference))
            [batch_id] = result.fetchone()

        insert_order_line = "INSERT INTO order_lines (orderid, sku, qty) VALUES (:orderid, :sku, :qty)"
        insert_allocation = "INSERT INTO allocations (orderline_id, batch_id) VALUES (:orderline_id, :batch_id)"
        select_order_line_id = "SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku AND qty=:qty"

        for order_line in batch._allocations:
            order_line_id = self.session.execute(select_order_line_id, dict(orderid=order_line.orderid, sku=order_line.sku, qty=order_line.qty)).fetchone()
            if not order_line_id:
                self.session.execute(insert_order_line, dict(orderid=order_line.orderid, sku=order_line.sku, qty=order_line.qty))
                [insert_order_line_id] = self.session.execute(select_order_line_id,
                                                dict(orderid=order_line.orderid, sku=order_line.sku,
                                                     qty=order_line.qty)).fetchone()
                self.session.execute(insert_allocation, dict(orderline_id=insert_order_line_id, batch_id=batch_id))

    def get(self, reference) -> model.Batch:
        select_query = "SELECT * FROM batches WHERE reference=:reference"
        result = self.session.execute(select_query, dict(reference=reference))
        row = result.fetchone()

        batch = model.Batch(row.reference, row.sku, row._purchased_quantity, row.eta)

        select_batch_order_lines_query = """
            SELECT order_lines.*
            FROM allocations
            JOIN order_lines ON allocations.orderline_id = order_lines.id
            JOIN batches ON allocations.batch_id = batches.id
            WHERE batches.reference = :batchid
            """
        result_order_lines = self.session.execute(select_batch_order_lines_query, dict(batchid=reference))
        rows = result_order_lines.fetchall()

        order_lines = set([model.OrderLine(row.orderid, row.sku, row.qty) for row in rows])
        batch._allocations = order_lines

        return batch
