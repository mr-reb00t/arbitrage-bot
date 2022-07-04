from decimal import Decimal
from exchanges.Order import OrderSide, Order, ImpossibleOrder
import logging
import copy

MAX_DEPTH = 8
logger = logging.getLogger("arbitrage_bot")

def arbitrage_helper(current_currency, current_amount,
					 current_path, currency_path, solutions, start_amount, start_currency, depth, min_profit=Decimal(0)):
	"""
	:param current_currency: exchange currency that is currently being hold
	:param current_amount: amount of exchange currency available for trading
	:param current_path: list of Orders to be made to achive this point
	:param currency_path: list of ExchangeCurrencies representing the current path
	:param solutions: list of valid solutions
	:param depth: current recursion depth
	:return:
	"""
	# if we reached maximum depth it means that we did not find a valid solution
	if depth >= MAX_DEPTH:
		return

	# check if we reached a valid solution
	if len(current_path) > 1 and start_currency.get_code() == current_currency.get_code():
		if current_amount > start_amount:
			# we found a valid solution
			# let's check if its profit is higher than the minimum
			profit = (current_amount / start_amount) - Decimal(1)

			if profit > min_profit:
				solutions.append(
					{
						"orders": current_path,
						"profit": profit
					}
				)

			return

	# we must try with all neighbour currencies
	for neighbour in current_currency:
		edge = current_currency.get_market(neighbour)
		market = edge["market"]

		# we do not want to start with a deposit
		# if there is an opportunity starting at another exchange, we will find it
		# it by searching on that exchange
		if len(current_path) == 0 and market.is_deposit():
			continue

		if edge["currency"] in currency_path:
			# we have already visited this node
			# however, we can visit it again if this node is the first one
			if edge["currency"] != currency_path[0]:
				continue

		# find if we are buying or selling
		side = OrderSide.SELL
		if market.get_quote_asset() == current_currency:
			# we have quote asset and we want base asset
			side = OrderSide.BUY

		# find which price should we look at
		# if we are buying BASE asset, we must use ASK price
		# if we are selling BASE asset, we must use BID price
		price = market.get_ask_price() if side == OrderSide.BUY else market.get_bid_price()

		if price is None:
			# price is not available at this time
			continue

		max_size = price.get_item()
		price = price.get_score()

		# to determine the order quantity we must know if we are buying or selling
		quantity = 0
		if side is OrderSide.BUY:
			# that means current currency is the quote asset
			quantity = current_amount / price
		else:
			# current currency is the base asset
			quantity = current_amount

		# quantity might be higher than the available size
		reduce = False
		if max_size < quantity:
			#logger.debug("Maximum market (%s) size is less than desired amount (%s < %s)", market.get_symbol(), max_size, quantity)
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
			# this order is not possible with current market limitations
			continue

		ref_current_path = current_path
		ref_start_amount = start_amount

		if reduce:
			# we must change all the previous amounts
			# make a copy of the current order path
			tmp_order_path = [order]

			try:
				for i in reversed(range(0, len(ref_current_path))):
					# we do not want to make deep copy
					order_copy = copy.copy(ref_current_path[i])

					if order_copy.is_deposit():
						order_copy.set_size(tmp_order_path[0].get_source_amount())
					else:
						order_copy.set_target_amount(tmp_order_path[0].get_source_amount())

					tmp_order_path.insert(0, order_copy)

				# initial amount might have changed
				ref_start_amount = tmp_order_path[0].get_source_amount()
				ref_current_path = tmp_order_path

				ref_current_path.pop()
			except ImpossibleOrder:
				# there is an order in the path that is impossible to make with this new size
				# therefore, we can discard this whole path
				continue


		# order quantity (or size) might have changed after making it valid
		# we must calculate the new target currency amount
		final_amount = order.get_target_amount()

		# then, we must subtract exchange fees (always taker fees as our orders should be completed instantly)
		final_amount *= (Decimal(1) - market.get_taker_fees())

		# make the recursion call
		new_path = ref_current_path.copy()
		new_path.append(order)

		new_currency_path = currency_path.copy()
		new_currency_path.append(edge["currency"])

		arbitrage_helper(
			edge["currency"],
			final_amount,
			new_path,
			new_currency_path,
			solutions,
			ref_start_amount,
			start_currency,
			depth + 1,
			min_profit
		)


def find_path(currency, amount, min_profit=Decimal(0)):
	# starting the depth search over a graph to find a valid solution
	solutions = []

	arbitrage_helper(
		currency,
		amount,
		[],
		[currency],
		solutions,
		amount,
		currency,
		1,
		min_profit
	)

	return solutions
