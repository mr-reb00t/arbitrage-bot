import abc
from threading import Lock
from heapq import heappush, heappop, heapify
from .ScoreItem import ScoreItem
from decimal import Decimal


class Market(object):
	"""
	A Market represents an exchange market in a particular exchange
	"""

	def __init__(self, base, quote, symbol, exchange,
				 maker_fee=Decimal(0), taker_fee=Decimal(0), deposit=False):
		# initializing mutex sections for ask and bid heaps
		self._askMutex = Lock()
		self._bidMutex = Lock()

		# initializing heaps
		self._askHeap = []
		self._bidHeap = []

		# market data
		self._base = base
		self._quote = quote
		self._symbol = symbol
		self._exchange = exchange

		self._makerFee = maker_fee
		self._takerFee = taker_fee

		self._rules = []

		# paths containing this market
		self._paths = []

		self._deposit = deposit

	def update_bid_price(self, price, quantity):
		"""
		Updates the bid price
		A bid is BUY offer (highest prices at the top)
		:param price: Decimal
		:param quantity: Decimal, if zero this price is not available
		:return: void
		"""
		with self._bidMutex:
			index = None

			for i in range(0, len(self._bidHeap)):
				if self._bidHeap[i].get_score() == price:
					index = i
					break

			# deciding to remove or update the price depending on the value
			if (quantity == 0) and (index is not None):
				# removing this value
				self._bidHeap.pop(index)

				# making sure that the list stays as a heap
				heapify(self._bidHeap)
			elif (quantity > 0) and (index is not None):
				# updating current quantity
				self._bidHeap[index].item = quantity
			else:
				# adding the item
				heappush(self._bidHeap, ScoreItem(price, quantity, inversed=True))

	def get_bid_price(self):
		"""
		Retrieves the best bid price for this market
		:return:
		"""
		value = None

		with self._bidMutex:
			if len(self._bidHeap) > 0:
				value = self._bidHeap[0]

		return value

	def update_ask_price(self, price, quantity):
		"""
		Updates the ask price
		An ask is a SELL order, lowest prices at the top
		:param price: Decimal
		:param quantity: Decimal, if zero this price is not available
		:return: void
		"""
		with self._askMutex:
			index = None

			for i in range(0, len(self._askHeap)):
				if self._askHeap[i].get_score() == price:
					index = i
					break

			# deciding to remove or update the price depending on the value
			if (quantity == 0) and (index is not None):
				# removing this value
				self._askHeap.pop(index)

				# making sure that the list stays as a heap
				heapify(self._askHeap)
			elif (quantity > 0) and (index is not None):
				# updating current quantity
				self._askHeap[index].item = quantity
			else:
				# adding the item
				heappush(self._askHeap, ScoreItem(price, quantity))

	def get_ask_price(self):
		"""
		Retrieves the best ask price for this market
		:return: Decimal or None if there is no price available
		"""
		value = None

		with self._askMutex:
			if len(self._askHeap) > 0:
				value = self._askHeap[0]

		return value

	def reset_prices(self):
		"""
		Resets market prices, removing any previous information about the current price
		:return:
		"""
		with self._askMutex:
			self._askHeap = []

		with self._bidMutex:
			self._bidHeap = []

	def get_base_asset(self):
		return self._base

	def get_quote_asset(self):
		return self._quote

	def get_taker_fees(self):
		return self._takerFee

	def set_taker_fees(self, value):
		self._takerFee = value

	def get_maker_fees(self):
		return self._makerFee

	def set_maker_fees(self, value):
		self._makerFee = value

	def __str__(self):
		return self._base.get_code() + self._quote.get_code()

	def get_rules(self):
		return self._rules

	def add_rule(self, rule):
		self._rules.append(rule)

	def get_symbol(self):
		return self._symbol

	def get_exchange(self):
		return self._exchange

	def is_deposit(self):
		return self._deposit

	def get_id(self):
		"""
		:return: string containing an unique market identifier
		"""
		return self._base.get_code() + self._quote.get_code() + "@" + self._exchange.get_name()

	def add_path(self, path):
		"""
		Adds a path containing this market to the list
		"""
		if path not in self._paths:
			self._paths.append(path)

	def scan_paths(self, initial_amount, min_profit=Decimal("0.01")):
		"""
		:return: list of profitable order sequences
		"""
		result = []
		for path in self._paths:
			aux = path.generate_orders(initial_amount)

			if aux is not None:
				start_order = aux[0]
				final_order = aux[len(aux) - 1]
				profit = final_order.get_target_amount(include_fees=True) / start_order.get_source_amount() - Decimal(1)

				if profit >= min_profit:
					result.append({ "profit": profit, "orders": aux })

		return result