"""Shared funworker producer/consumer helpers for source batch tasks."""

from collections import Counter
from typing import Any, Iterator, List

from funworker import BaseConsumer, BaseProducer


class ListProducer(BaseProducer):
    """Produce items from a pre-computed list, then stop."""

    def __init__(self, output_queue, items: List[Any], **kwargs):
        super().__init__(output_queue=output_queue, **kwargs)
        self._iterator: Iterator[Any] = iter(items)

    def produce(self) -> Any:
        return next(self._iterator)


class CounterConsumer(BaseConsumer):
    """Tally string status results produced by a processing stage."""

    def __init__(self, input_queue, **kwargs):
        super().__init__(input_queue=input_queue, **kwargs)
        self.counts: Counter = Counter()

    def consume(self, item: Any) -> None:
        self.counts[item] += 1
