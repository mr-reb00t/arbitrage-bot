from enum import Enum
from decimal import Decimal
import logging
import uuid

logger = logging.getLogger("arbitrage_bot")

class OrderSide(Enum):
	BUY = 1
	SELL = 2

class OrderStatus(Enum):
	REJECTED = 1
	PENDING = 2
	COMPLETED = 3

class ImpossibleOrder(Exception):
	"""
	This exception indicates that is not possible to convert an order into a valid order with
	current market rules
	"""
	def __init__(self):
		Exception.__init__(self)


class Order(object):
	def __init__(self, price, quantity, side, market, maximum_amount=Decimal(0), minimum_amount=Decimal(0)):
		# defining order details
		self._price = price
		self._quantity = quantity
		self._side = side

		# market whether this order should be posted
		self._market = market

		# set maximum quantity available
		self._maximum = maximum_amount

		# set minimum amount, useful if we want to get at least some target amunt
		self._minimum = minimum_amount

		# generating an unique identifier for this order
		# it will be generated once we get it for the first time
		self._id = None

		# sequence identifier
		self._sId = None

	def make_valid(self):
		"""
		Makes this order complaint with the market restrictions
		:return: void
		"""
		MAX = 100
		market_rules = self._market.get_rules()
		changes = True
		iterations = 0
		while changes and (iterations < MAX):
			changes = False

			for rule in market_rules:
				changes = (rule.make_valid(self)) or changes

			iterations += 1

		if iterations >= MAX:
			raise ImpossibleOrder()

	def get_target_amount(self, include_fees=False):
		"""
		Returns the amount of the currency that will be given after order execution
		:return: Decimal
		"""
		if self._side == OrderSide.BUY:
			return self._quantity if not include_fees else self._quantity * (Decimal(1) - self._market.get_taker_fees())
		else:
			value = self._quantity * self._price
			return value if not include_fees else value * (Decimal(1) - self._market.get_taker_fees())

	def get_target_currency(self):
		"""
		Returns the ExchangeCurrency that will be obtained after order execution
		:return: ExchangeCurrency object
		"""
		if self._side == OrderSide.BUY:
			return self._market.get_base_asset()
		else:
			return self._market.get_quote_asset()

	def set_target_amount(self, amount, include_fees=True):
		"""
		Modifies order's size to get, at leat, amount of target currency coins
		It might throw a ImpossibleOrder exception if it is impossible to achieve the target amount
		:param amount: desired target amount
		:param include_fees: indicates whether fees must be considered or not
		"""
		if include_fees:
			# we will always be taker in a trade as we want to get the order filled instantaneously
			multiplier = Decimal(1) - self._market.get_taker_fees()
		else:
			multiplier = Decimal(1)

		if self._side == OrderSide.BUY:
			new_size = amount / multiplier
		else:
			if self._price.is_zero():
				raise ImpossibleOrder()

			new_size = amount / (self._price * multiplier)

		if new_size > self._maximum:
			# this order is impossible to make valid
			raise ImpossibleOrder()

		self._quantity = new_size
		self._minimum = new_size

		self.make_valid()

	def get_source_amount(self):
		"""
		Returns the amount of the currency needed for the order execution
		"""
		if self._side == OrderSide.BUY:
			# we need QUOTE currency
			return self._quantity * self._price
		else:
			return self._quantity

	def get_source_currency(self):
		"""
		Returns the ExchangeCurrency needed for the order execution
		"""
		if self._side == OrderSide.BUY:
			return self._market.get_quote_asset()
		else:
			return self._market.get_base_asset()

	def can_be_executed(self):
		"""
		:return: whether this order can be executed with current balance
		"""
		return self.get_source_currency().get_balance() >= self.get_source_amount()

	def set_size(self, size):
		self._quantity = size

	def __str__(self):
		"""
		String representation of an order
		:return:
		"""
		tmp = "BUY" if self._side == OrderSide.BUY else "SELL"
		tmp += " @ " + str(self._market)
		tmp += " (" + self._market.get_exchange().get_name() + ")"
		tmp += " - " + str(self._price)
		tmp += " - " + str(self._quantity)
		return tmp

	def get_size(self):
		return self._quantity

	def set_size(self, quantity):
		self._quantity = quantity

	def get_maximum_size(self):
		return self._maximum

	def get_minimum_size(self):
		return self._minimum

	def get_price(self):
		"""
		:return: order's price
		"""
		return self._price

	def get_side(self):
		"""
		:return: order's side, it can be an OrderSide.BUY or an OrderSide.SELL
		"""
		return self._side

	def get_symbol(self):
		return self._market.get_symbol()

	def get_exchange(self):
		return self._market.get_exchange().get_name()

	def is_deposit(self):
		return self._market.is_deposit()

	def get_id(self):
		if self._id is None:
			self._id = str(uuid.uuid4())

		return self._id

	def execute(self):
		self._market.get_exchange().make_order(self)
		
	def generate_request(self):
		return self._market.get_exchange().generate_order_request(self)

	def set_sequence_id(self, id):
		self._sId = id

	def get_sequence_id(self):
		return self._sId