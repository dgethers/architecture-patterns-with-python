import abc
import model
from typing import List


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError

    @abc.abstractmethod
    def list(self) -> List[model.Batch]:
        raise NotImplementedError

    @abc.abstractmethod
    def order_line_by_orderid_and_sky(self, sku, orderid) -> model.OrderLine:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        self.session.add(batch)

    def get(self, reference):
        return self.session.query(model.Batch).filter_by(reference=reference).one()

    def list(self):
        return self.session.query(model.Batch).all()

    def order_line_by_orderid_and_sky(self, sku, orderid) -> model.OrderLine:
        return self.session.query(model.OrderLine).filter_by(sku=sku, orderid=orderid).one()

