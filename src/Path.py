from exchanges.Order import OrderSide, Order, ImpossibleOrder
from decimal import Decimal
import copy


class Path(object):
	def __init__(self):
		# ordered list of currencies
		self._currencies = []
		self._orders = []

	def add_currency(self, currency):
		self._currencies.append(currency)

		if len(self._currencies) > 1:
			# create a future order structure with the two last currencies
			previous_currency = self._currencies[len(self._currencies) - 2]

			# TODO: currency.get_code() will need to be changed to currency.get_id() once edges use an ID system
			edge = previous_currency.get_market(currency.get_code())
			market = edge["market"]

			# determine whether it is a sell or a buy
			if market.get_base_asset() == previous_currency:
				# this edge is base -> quote (quote is currency)
				side = OrderSide.SELL
			else:
				# this edge is quote -> base (base is currency)
				side = OrderSide.BUY

			future_order = {
				"side": side,
				"market": market
			}

			self._orders.append(future_order)

			# add this path to the market's list
			market.add_path(self)

	def generate_orders(self, initial_amount):
		"""
		:param initial_amount: Decimal indicating the initial order amount
		:return: list of orders, including trasfers between exchanges
		"""
		orders = []

		# amount of currency being held
		current_amount = initial_amount

		for x in self._orders:
			market = x["market"]
			side = x["side"]

			# find which price should we look at
			# if we are buying BASE asset, we must use ASK price
			# if we are selling BASE asset, we must use BID price
			price = market.get_ask_price() if side == OrderSide.BUY else market.get_bid_price()

			if price is None:
				# we do not have an available price, we can't execute this path
				return None

			max_size = price.get_item()
			price = price.get_score()

			# to determine the order quantity we must know if we are buying or selling
			if side is OrderSide.BUY:
				# that means current currency is the quote asset
				quantity = current_amount / price
			else:
				# current currency is the base asset
				quantity = current_amount

			reduce = False
			if max_size < quantity:
				reduce = True
				quantity = max_size

			order = Order(
				price,
				quantity,
				side,
				market,
				maximum_amount=Decimal(quantity)
			)

			try:
				order.make_valid()
			except ImpossibleOrder:
				# this path is impossible to execute
				return None

			if reduce:
				# we must revise all previous orders
				tmp_orders = [order]

				try:
					for i in reversed(range(0, len(orders))):
						# we do not want to make deep copy
						order_copy = copy.copy(orders[i])

						if order_copy.is_deposit():
							order_copy.set_size(orders[0].get_source_amount())
						else:
							order_copy.set_target_amount(orders[0].get_source_amount())

						tmp_orders.insert(0, order_copy)

					orders = tmp_orders
				except ImpossibleOrder:
					# there is an order in the path that is impossible to make with this new size
					# therefore, we can discard this whole path
					return None
			else:
				orders.append(order)

			# prepare for next iteration
			current_amount = order.get_target_amount(include_fees=True)

		return orders

	def __str__(self):
		return " - ".join(map(lambda x: x.get_id(), list(self._currencies)))


def generate_all_paths(start_currency, max_depth):
	def helper(start_currency, current_currency, current_path, depth, valid_paths):
		"""
		Recursive function to generate all valid paths
		:param start_currency: initial ExchangeCurrency
		:param current_currency: current ExchangeCurrency
		:param current_path: list of visited ExchangeCurrencies
		:param depth: current recursion depth
		:param valid_paths: list of valid paths
		"""
		if depth > max_depth:
			# we do not want solutions longer than max_depth, this path is invalid
			return

		if (len(current_path) > 1) and (start_currency.get_code() == current_currency.get_code()):
			# this is a valid path, create a new Path object and copy the current_path ExchangeCurrnecies
			new_path = Path()

			for currency in current_path:
				new_path.add_currency(currency)

			# and add this path to valid_paths
			valid_paths.append(new_path)
			return

		# we might have more neighbors to visit
		for neighbor in current_currency:
			# neighbor is the code of an ExchangeCurrency
			edge = current_currency.get_market(neighbor)
			market = edge["market"]
			neighbor_currency = edge["currency"]

			# we do not want to start a path visiting the same currency but at another exchange, as this
			# edge represents a deposit
			if (len(current_path) == 1) and market.is_deposit():
				# however, we want to check other neighbors
				continue

			# we do not want to visit ExchangeCurrencies already visited
			if neighbor_currency in current_path:
				# unless this currency is the initial currency
				if current_path[0] != neighbor_currency:
					continue

			new_path = current_path.copy()
			new_path.append(neighbor_currency)

			helper(
				start_currency,
				neighbor_currency,
				new_path,
				depth + 1,
				valid_paths
			)

	paths = []

	helper(
		start_currency,
		start_currency,
		[start_currency],
		1,
		paths
	)

	return paths