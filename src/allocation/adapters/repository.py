import abc
from allocation.domain import model


class AbstractRepository(abc.ABC):
    def add(self, product: model.Product):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, sku: str) -> model.Product:
        raise NotImplementedError


class SqlAlchemyProductRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, product: model.Product):
        self.session.add(product)

    def get(self, sku: str) -> model.Product:
        return self.session.query(model.Product).filter_by(sku=sku).first()
