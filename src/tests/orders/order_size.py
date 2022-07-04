import unittest
from decimal import Decimal
from src.exchanges.rules.SizeRule import SizeRule
from src.exchanges.Market import Market
from src.exchanges.Order import Order, OrderSide, ImpossibleOrder

class OrderSizeTest(unittest.TestCase):
	def test_order_sides(self):
		"""
		Create orders with SELL and BUY sides.
		Checks if target and source amounts are correct
		"""
		order_buy = Order(
			Decimal("100.00"),
			Decimal("55"),
			OrderSide.BUY,
			None
		)

		self.assertEqual(
			order_buy.get_target_amount(),
			Decimal("55")
		)

		self.assertEqual(
			order_buy.get_source_amount(),
			Decimal("55") * Decimal("100")
		)

		order_sell = Order(
			Decimal(100),
			Decimal(55),
			OrderSide.SELL,
			None
		)

		self.assertEqual(
			order_sell.get_target_amount(),
			Decimal("55") * Decimal("100")
		)

		self.assertEqual(
			order_sell.get_source_amount(),
			Decimal("55")
		)


	def test_minimum_size(self):
		"""
		Create an order with a minimum size and corrects it
		Create an order with an impossible minimum size
		"""
		market = Market(
			None,  # base
			None,  # quote
			"",  # symbol
			None  # exchange
		)

		rule = SizeRule(
			Decimal(1),  # min amount
			Decimal(0),  # max amount
			Decimal("0.1")  # step size
		)
		market.add_rule(rule)

		order = Order(
			Decimal("100"),
			Decimal("0.5"),
			OrderSide.BUY,
			market,
			maximum_amount=Decimal(999)
		)

		order.make_valid()
		self.assertGreaterEqual(order.get_size(), Decimal("1"))

		order = Order(
			Decimal("100"),
			Decimal("0.5"),
			OrderSide.BUY,
			market,
			maximum_amount=Decimal("0.75")
		)
		self.assertRaises(ImpossibleOrder, order.make_valid)

	def test_maximum_amount(self):
		"""
		Creates an order with a size higher than market's maximum
		"""
		market = Market(
			None,  # base
			None,  # quote
			"",  # symbol
			None  # exchange
		)

		rule = SizeRule(
			Decimal(0),  # min amount
			Decimal(10),  # max amount
			Decimal("0.1")  # step size
		)
		market.add_rule(rule)

		order = Order(
			Decimal("100"),
			Decimal("50"),
			OrderSide.BUY,
			market,
			maximum_amount=Decimal(1000)
		)
		order.make_valid()
		self.assertLessEqual(order.get_size(), Decimal(10))

	def test_with_unlimited_size(self):
		"""
		Create an order with a size not multiple of step size with an infinite order size
		"""
		market = Market(
			None, # base
			None, # quote
			"", # symbol
			None # exchange
		)

		rule = SizeRule(
			Decimal(0), # min amount
			Decimal(0), # max amount
			Decimal("0.1") # step size
		)

		market.add_rule(rule)

		# creating the order
		order = Order(
			Decimal("100.00"), # price
			Decimal("0.1234"), # size,
			OrderSide.BUY,
			market,
			maximum_amount=Decimal("99999")
		)

		order.make_valid()

		self.assertEqual(order.get_size() % Decimal("0.1"), 0)

	def test_set_target_amount(self):
		market = Market(
			None,  # base
			None,  # quote
			"",  # symbol
			None  # exchange
		)

		order = Order(
			Decimal("100.00"),
			Decimal("10"),
			OrderSide.BUY,
			market,
			maximum_amount=Decimal("99999")
		)

		order.set_target_amount(Decimal("20"), include_fees=False)
		self.assertEqual(order.get_target_amount(), Decimal("20"))
		self.assertEqual(order.get_source_amount(), Decimal("2000"))


		order = Order(
			Decimal("100.00"),
			Decimal("10"),
			OrderSide.SELL,
			market,
			maximum_amount=Decimal("99999")
		)

		order.set_target_amount(Decimal("10"), include_fees=False)
		self.assertEqual(order.get_target_amount(), Decimal("10"))
		self.assertEqual(order.get_source_amount(), Decimal("0.1"))

if __name__ == '__main__':
	unittest.main()