import sqlite3
import logging
import threading
from decimal import Decimal
import time
from math import floor

logger = logging.getLogger("arbitrage_bot")

class Database(object):
	def __init__(self, file_name="app.db"):
		# create a connection and allow executions from different threads
		self._connection = sqlite3.connect(file_name, check_same_thread=False)

		# mutex to serialze all access to the database
		self._mutex = threading.Lock()

		with self._mutex:
			logger.debug("Initializing database")
			self._connection.execute("CREATE TABLE IF NOT EXISTS transfers (" +
										 "id INT AUTO_INCREMENT," +
										 "amount DECIMAL(15, 8)," +
										 "currency VARCHAR(32)," +
										 "source VARCHAR(32)," +
										 "target VARCHAR(32)," +
										 "time INTEGER" +
									 ");")

			self._connection.execute("CREATE TABLE IF NOT EXISTS sequences (" +
										 "id VARCHAR(40) PRIMARY KEY," +
										 "initial_amount DECIMAL(15, 8)," +
										 "final_amount DECIMAL(15, 8)," +
										 "profit DECIMAL(15, 8)," +
										 "time INTEGER" +
									 ")")



			self._connection.commit()

	def add_transfer(self, order):
		"""
		Indicates that a deposit from source has been made to target
		:param order: order that represents this transfer between exchanges
		"""
		amount = order.get_size()
		currency_code = order.get_source_currency().get_code()
		source = order.get_source_currency().get_exchange_name()
		target = order.get_target_currency().get_exchange_name()

		with self._mutex:
			self._connection.execute(
				"INSERT INTO transfers VALUES (NULL, ?, ?, ?, ?, ?)",
				[str(amount), currency_code, source, target, floor(time.time())]
			)

			self._connection.commit()

	def add_sequence(self, id, order_list):
		"""
		Stores the orders and the sequence in the database
		"""
		initial_amount = order_list[0][0].get_source_amount()

		idx = len(order_list) - 1
		final_amount = order_list[idx][len(order_list[idx]) - 1].get_target_amount(include_fees=True)

		profit = final_amount / initial_amount - Decimal(1)

		with self._mutex:
			self._connection.execute(
				"INSERT INTO sequences VALUES (?, ?, ?, ?, ?)",
				[id, str(initial_amount), str(final_amount), str(profit), floor(time.time())]
			)
			self._connection.commit()
