from unittest import mock
import pytest
from allocation.adapters import repository
from allocation.domain import events
from allocation.service_layer import  unit_of_work, messagebus


class FakeRepository(repository.AbstractRepository):
    def __init__(self, products):
        super().__init__()
        self._products = set(products)

    def _add(self, product):
        self._products.add(product)

    def _get(self, sku):
        return next((p for p in self._products if p.sku == sku), None)


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.products = FakeRepository([])
        self.committed = False

    def _commit(self):
        self.committed = True

    def rollback(self):
        pass


class TestAddBatch:

    def test_add_new_product(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("batch1", "CRUNCHY-ARMCHAIR", 100, None), uow)
        assert uow.products.get("CRUNCHY-ARMCHAIR") is not None
        assert uow.committed


class TestAllocate:

    def test_returns_allocation(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("batch1", "COMPLICATED-LAMP", 100, None), uow)
        [result] = messagebus.handle(events.AllocationRequired("o1", "COMPLICATED-LAMP", 10), uow)
        assert result == "batch1"

