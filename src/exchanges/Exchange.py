import abc
from .ExchangeCurrency import ExchangeCurrency
from .Market import Market
from decimal import Decimal
import logging
from threading import Condition

logger = logging.getLogger("arbitrage_bot")

class Exchange(metaclass=abc.ABCMeta):
	def __init__(self, name, available_currencies={}, app=None):
		self._name = name
		self._availableCurrencies = available_currencies
		self._app = app

		# information about markets and currencies
		self._currencies = {}
		self._markets = {}

		# listeners
		self._listeners = {}
		self._listeners["orderUpdate"] = []
		self._listeners["balanceUpdate"] = []

		self._stop = False

		# condition variable
		self._readyCondition = Condition()
		self._ready = False

	def __iter__(self):
		self._iterator = self._markets.keys().__iter__()
		return self._iterator

	def __next__(self):
		return next(self._iterator)

	def get_market(self, market_name):
		return self._markets[market_name]

	def find_or_create_exchange_currency(self, currency):
		"""
		Finds or create an Exchange Currency for this exchange
		:param currency:
		:return:
		"""
		if currency.code not in self._currencies:
			self._currencies[currency.code] = ExchangeCurrency(
				currency,
				self
			)

		return self._currencies[currency.code]

	def add_deposit(self, currency, exchange):
		"""
		Adds a deposit method from an exchange to this one
		:param currency: Currency to add as a deposit mehtod
		:param exchange:
		"""

		# create a fake market
		origin_currency = self._currencies[currency.code]
		target_currency = exchange._currencies[currency.code]

		market = Market(origin_currency, target_currency, "deposit", self, deposit=True)
		market.update_bid_price(Decimal(1), Decimal(9999999))
		market.update_ask_price(Decimal(1), Decimal(9999999))

		# adding edge between nodes
		origin_currency.add_neighbour_currency(target_currency, market)

	@abc.abstractmethod
	def initialize(self):
		pass

	@abc.abstractmethod
	def stop(self):
		"""
		This method should close all open connections with any open websocket server
		:return: nothing
		"""
		pass

	@abc.abstractmethod
	def fetch(self, endpoint, method="GET", authentication=False, signature=False, headers={}, parameters=None):
		"""
		Makes a HTTP request to an exchange with the speciefied options
		:param headers: dict of parameters (name: value)
		:param signature: boolean indicating whether this request should be signed
		:param authentication: boolean indicating whether this request should include user authentication
		:param method: GET / POST / DELETE / PUT
		:param endpoint: string
		:return: JSON
		"""
		pass

	@abc.abstractmethod
	def make_order(self, order, test=False):
		"""
		Creates the specified order in its exchange
		:param order: a valid order for this exchange
		"""
		pass

	@abc.abstractmethod
	def generate_order_request(self, order, test=False):
		"""
		Generates a request structure to make an order on the exchange
		It does not execute the order
		:param order: order to be generated
		:return: request structure
		"""
		pass

	def to_dot_file(self):
		"""
		Saves the current exchange state into a DOT file format
		"""

		file = open(self._name + ".dot", "w")
		file.write("digraph G {\n")

		# pending nodes to be visited / drawn
		pending = []

		for x in self._currencies:
			pending.append(self._currencies[x])
			break

		visited = {}

		while len(pending) > 0:
			# item is an ExchangeCurrency instance
			item = pending.pop()
			visited[item.get_id()] = True

			for target_currency in item:
				# target_currency is a string
				edge = item.get_market(target_currency)
				market = edge["market"]

				# we must visit this currency in the future
				currency_id = edge["currency"].get_id()
				if currency_id not in visited:
					visited[currency_id] = False

				if not visited[currency_id]:
					pending.append(edge["currency"])

				if market.get_base_asset().get_code() == item.get_code():
					# this is a sell edge (BASE -> QUOTE)
					bid = market.get_bid_price()
					price = bid.get_score() if bid is not None else 0
				else:
					# this is a buy edge (QUOTE -> BASE)
					ask = market.get_ask_price()
					price = (1 / ask.get_score()) if ask is not None else 0

				# adding the edge between item and this new target currency
				file.write("\t" + item.get_id() + " -> " + currency_id + " [label=\""+ str(price) + "\"]")

		file.write("}")
		file.close()

	def add_listener(self, event_name, fn):
		"""
		Adds an event listener for the specified event name
		:param event_name:
		:param fn: function that will be called once the event_name occurs
			- orderUpdate: fn(order, status)
			- balanceUpdate: fn(exchange, currency, newAmount)
		"""
		if event_name in self._listeners:
			self._listeners[event_name].append(fn)

	def invoke_listener(self, event_name, *args):
		"""
		Invokes this Exchange listeners
		:param event_name:
		:param args: listener arguments
		"""
		if event_name in self._listeners:
			for listener in self._listeners[event_name]:
				listener(*args)

	def get_name(self):
		return self._name

	def wait_until_ready(self):
		"""
		Waits until this exchange has been initialized. The initialisation process will conclude once, at least, the
		markets have been created. WebSocket streams might not have been created
		"""
		with self._readyCondition:
			while not self._ready:
				self._readyCondition.wait()

	def set_ready(self, value):
		"""
		Changes the ready state for the exchange. It awakens all the waiting threads from the Condition variable
		These threads might have to wait again if value is False
		:param value: new value of the ready variable
		"""
		with self._readyCondition:
			self._ready = value
			self._readyCondition.notify_all()