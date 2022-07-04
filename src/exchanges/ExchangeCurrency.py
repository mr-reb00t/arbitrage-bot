from decimal import Decimal

class ExchangeCurrency(object):
	def __init__(self, currency, exchange):
		# basic information about this currency on a particular exchange
		self._currency = currency
		self._exchange = exchange

		# neighbour nodes: dictionary with keys as target currencies
		self._edges = {}

		# current balance of this currency
		self._balance = Decimal(0)

		# iterator
		self._it = None

	def add_neighbour_currency(self, currency, market):
		"""
		Adds an edge between two currencies from an exchange
		:param currency: target ExchangeCurrency
		:param market: the market that represents this edge
		:return: void
		"""
		if currency.get_code() not in self._edges:
			self._edges[currency.get_code()] = {
				"currency": currency,
				"market": market
			}

			# adding this edge to the target currency
			currency.add_neighbour_currency(self, market)

	def get_market(self, target_currency):
		"""
		:param target_currency: currency code
		:return: edge between this currency and the target currency
		"""
		if target_currency in self._edges:
			return self._edges[target_currency]
		else:
			return None

	def get_code(self):
		"""
		:return: currency code identifier
		"""
		return self._currency.code

	def get_id(self):
		"""
		:return: currency identifier (code_exchange)
		"""
		return self._currency.code + "_" + self._exchange.get_name()

	def __iter__(self):
		"""
		Iteration is made over neighbour edges
		:return:
		"""
		self._it = self._edges.keys().__iter__()
		return  self._it

	def __next__(self):
		return next(self._it)

	def get_balance(self):
		"""
		:return: balance of this currency on an exchange
		"""
		return self._balance

	def set_balance(self, value):
		"""
		:param value: amount of this currency
		"""
		self._balance = value

	def add_balance(self, value):
		"""
		Increments (or decrements) the amount of a currency
		:param value: delta balance
		"""
		self._balance += value

	def get_exchange_name(self):
		return self._exchange.get_name()