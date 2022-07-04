from .Rule import Rule
from ..Order import ImpossibleOrder
import logging

logger = logging.getLogger("arbitrage_bot")

class SizeRule(Rule):
	def __init__(self, minimum, maximum, step):
		self._minimum = minimum
		self._maximum = maximum
		self._step = step

	def make_valid(self, order):
		# this order will be impossible if minimum is higher than the maximum order size
		if (self._minimum > 0) and (self._minimum > order.get_maximum_size()):
			#logger.error("INVALID ORDER SIZERULE (%f > %f) %s - %s" % (self._minimum, order.get_maximum_size(), order.get_symbol(), order.get_exchange()))
			raise ImpossibleOrder()

		if (self._maximum > 0 and self._maximum < order.get_minimum_size()) or (order.get_maximum_size() < order.get_minimum_size()):
			raise ImpossibleOrder()

		changes = False

		# change minimum amount
		if (self._minimum > 0) and (self._minimum > order.get_size()):
			order.set_size(self._minimum)
			changes = True

		# change maximum amount
		if(self._maximum > 0) and (self._maximum < order.get_size()):
			order.set_size(self._maximum)
			changes = True

		# check if size is a multiple of step
		if self._step != 0:
			mod = order.get_size() % self._step

			if mod != 0:
				first_option = order.get_size() - mod

				if first_option >= order.get_minimum_size() and first_option < order.get_maximum_size():
					order.set_size(first_option)
				else:
					second_option = order.get_size() + mod
					if second_option >= order.get_minimum_size() and second_option < order.get_maximum_size():
						order.set_size(second_option)
					else:
						raise ImpossibleOrder()

				changes = True

		return changes
