from .Rule import Rule
from ..Order import ImpossibleOrder
from decimal import Decimal
import logging

logger = logging.getLogger("arbitrage_bot")

class ValueRule(Rule):
	def __init__(self, min_value):
		self._min_value = min_value

	def make_valid(self, order):
		# price * amount >= self._min_value
		value = order.get_price() * order.get_size()

		if value < self._min_value:
			# order is not valid, we must make some changes
			# we only can increase the size
			min_size = self._min_value / order.get_price()
			if min_size > order.get_maximum_size():
				# we can't convert the order to a valid order
				raise ImpossibleOrder()

			order.set_size(min_size)

		return False