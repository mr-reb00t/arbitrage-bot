import time
import json
import os
import sys
import threading
import logging
from rich import print
from rich.progress import Progress
from rich.console import Console
from rich.table import Table
from decimal import Decimal
from exchanges.binance.Binance import BinanceExchange
from exchanges.poloniex.Poloniex import PoloniexExchange
from exchanges.Currency import Currency
from Arbitrage import find_path
from exchanges.Order import Order, OrderSide, OrderStatus
from Database import Database
from multiprocessing import Queue, cpu_count
import queue
from Worker import Worker
from Path import Path, generate_all_paths
import uuid



logger = logging.getLogger("arbitrage_bot")
SCAN_INTERVAL = 0.1 # how often the bot looks for new opportunities (in seconds)

all_currencies = {
	"BTC": 	Currency("BTC", "Bitcoin"),
	"USDT": Currency("USDT", "USD Token"),
	"USDC": Currency("USDC", "USD Coin"),
	"ETH": Currency("ETH", "Ethereum"),
	"LTC": Currency("LTC", "Litecoin"),
	"BUSD": Currency("BUSD", "Binance USD")
}

class App(object):
	def __init__(self):
		# loading settings
		try:
			self.currencies = {}
			self.load_settings()
		except Exception as e:
			logger.error(e)
			logger.error("Could not load 'config/settings.json' file")
			sys.exit()

		# initialize workers
		self._tasks = Queue()
		self._workers = []

		workers = cpu_count()
		for i in range(0, workers):
			worker = Worker(i, self._tasks)
			worker.start()
			self._workers.append(worker)

		# arbitrage pending scans
		self._pendingScanMutex = threading.Lock()
		self._pendingScans = queue.Queue()
		self._pendingScansList = {}

		# initializing exchanges
		self.initialize_exchanges()
		self.generate_paths()

		# stop synchronization
		self.stop = False

		# order sequence
		self.sequencesMutex = threading.Lock()
		self.sequences = {}
		self.order_sequence = {}
		self.orders = {}
		self.started_order = False
		self.currentSequences = 0
		self.lastSequenceStarted = 0

		# trading enabled
		self.trading_enabled = False

		# arbitrage thread
		self._arbitrage_thread = threading.Thread(target=self.arbitrage_worker, name="ArbitrageWorker")
		self._arbitrage_thread.start()

		def gui_thread():
			try:
				self.gui()
			except KeyboardInterrupt:
				self.stop()

		self._guiThread = threading.Thread(target=gui_thread, name="GUI Thread")
		#self._guiThread.start()

		# storage database
		self.db = Database("app.db")

	def load_settings(self):
		"""
		Loads App settings
		"""
		path = os.path.dirname(os.path.realpath(__file__)) + "/config/settings.json"
		file = open(path)
		data = json.loads(file.read())

		# load max order amount
		self.orderMaxAmount = Decimal(data["orderMaxAmount"])
		self.minProfitAmount = Decimal(data["minProfit"]) / Decimal(100)

		# allow sequential sequences
		self.sequentialSequencesEnabled = data["allowSequentialSequences"]

		# multiple sequences at the same time
		self.multipleSequences = data["multipleSequences"]
		self.maximumSequences = data["maximumSequences"]
		self.timeBetweenSequences = data["timeBetweenSequences"]

		self.maxDepth = data["maxDepth"]

		self.enabledExchanges = data["exchanges"]
		logger.info("Enabled exchanges are: %s", ",".join(self.enabledExchanges))

		# loading currencies
		loaded_currencies = []
		for currency in data["enabledCurrencies"]:
			if currency in all_currencies:
				loaded_currencies.append(currency)
				self.currencies[currency] = all_currencies[currency]
			else:
				logger.error("Currency %s not recognized", currency)

		logger.info("Enabled currencies: %s", ",".join(loaded_currencies))

		file.close()

		# printing the information loaded
		logger.info("Maximum order amount is %s", self.orderMaxAmount)
		logger.info("Minimum profit amount is %s %%", (self.minProfitAmount * 100))
		logger.info("Maximum path depth is %d", self.maxDepth)

		if self.multipleSequences:
			if self.maximumSequences == 0:
				logger.info("Maximum number of sequences simulteneously: unlimited")
			else:
				logger.info("Maximum number of sequences simulteneously: %d", self.maximumSequences)
			logger.info("Time between sequences: %s seconds", self.timeBetweenSequences)
		else:
			logger.info("Only one sequence will be executed at once")

		logger.info("Sequences with sequential orders are enabled: %s", self.sequentialSequencesEnabled)

	def initialize_exchanges(self):
		exchanges = [
			{
				"file": "binance.json",
				"fn": BinanceExchange
			},

			{
				"file": "poloniex.json",
				"fn": PoloniexExchange
			}
		]

		self.exchanges = {}

		for exchange in exchanges:
			# read exchange file
			try:
				with open(os.path.dirname(__file__) + "/config/" + str(exchange["file"]), "r") as file:
					data = json.loads(file.read())

					if data["name"] not in self.enabledExchanges:
						continue

					tmp = None
					if ("api" in data) and ("secret" in data):
						tmp = exchange["fn"](api=data["api"], secret=data["secret"], app=self, available_currencies=self.currencies)

					self.exchanges[data["name"]] = tmp
			except FileNotFoundError as error:
				logger.error("Couldn't open file %s, ignoring exchange.", error.filename)

		for x in self.exchanges:
			self.exchanges[x].add_listener("orderUpdate", self.on_order_update)
			self.exchanges[x].initialize()

		# creating deposit methods between exchanges
		if "Poloniex" in self.exchanges:
			self.exchanges["Poloniex"].add_deposit(self.currencies["USDT"], self.exchanges["Binance"])
			self.exchanges["Poloniex"].add_deposit(self.currencies["BTC"], self.exchanges["Binance"])
			self.exchanges["Poloniex"].add_deposit(self.currencies["ETH"], self.exchanges["Binance"])
			self.exchanges["Poloniex"].add_deposit(self.currencies["LTC"], self.exchanges["Binance"])

		# wait until exchanges are ready
		for exchange in self.exchanges:
			self.exchanges[exchange].wait_until_ready()

		logger.info("Exchanges are ready")

	def generate_paths(self):
		"""
		Generate valid arbitrage paths
		"""
		valid_paths = []

		for exchange in self.exchanges:
			start_currency = self.exchanges[exchange].find_or_create_exchange_currency(self.currencies["USDT"])
			paths = generate_all_paths(start_currency, self.maxDepth)

			for x in paths:
				valid_paths.append(x)

		for path in valid_paths:
			logger.debug(str(path))

		logger.info("Generated %d possible paths" % (len(valid_paths)))


	def gui(self):
		while not self.stop:
			# printing prices for each of the exchangers available
			table = Table()
			table.add_column("Pair")

			pairs = {}

			for exchange in self.exchanges:
				table.add_column(exchange)

				for market in self.exchanges[exchange]:
					if market not in pairs:
						pairs[market] = {}

					pairs[market][exchange] = {
						"ask": self.exchanges[exchange].get_market(market).get_ask_price(),
						"bid": self.exchanges[exchange].get_market(market).get_bid_price()
					}

			for pair in pairs:
				row = [pair]

				for exchange in self.exchanges:
					if exchange not in pairs[pair]:
						row.append("-")
					else:
						cell = ""

						if pairs[pair][exchange]["bid"] is not None:
							bid_price = pairs[pair][exchange]["bid"].get_score()
						else:
							bid_price = "NA"

						cell += "Bid: " + str(bid_price) + "\n"

						if pairs[pair][exchange]["ask"] is not None:
							ask_price = pairs[pair][exchange]["ask"].get_score()
						else:
							ask_price = "NA"

						cell += "Ask: " + str(ask_price)
						row.append(cell)

				table.add_row(*row)

			#Console().print(table)

			time.sleep(1)

	def arbitrage_worker(self):
		logger.debug("Arbitrage thread has started")

		while not self.stop:
			market = self._pendingScans.get()

			if market is None:
				# this is to stop the thread
				continue

			with self._pendingScanMutex:
				# remove the market from the scan list
				del self._pendingScansList[market.get_id()]

			# scan all paths linked to this market, if we can create new orders
			now = time.time()

			can_scan = ((not self.multipleSequences) and (self.currentSequences == 0)) or \
					   (self.multipleSequences and ((now - self.lastSequenceStarted) > self.timeBetweenSequences) \
						and ((self.maximumSequences == 0) or (self.currentSequences < self.maximumSequences)))

			if can_scan:
				solutions = market.scan_paths(self.orderMaxAmount, min_profit=self.minProfitAmount)

				# best arbitrage opportunity
				highest_profit = -1
				highest_solution = None

				for x in solutions:
					logger.info("Profit %f" % (x["profit"] * 100))

					if x["profit"] > highest_profit:
						highest_solution = x["orders"]
						highest_profit = x["profit"]

				if highest_solution is not None and can_scan:
					try:
						# avoid creating multiple order at the same time
						self.started_order = True

						for o in highest_solution:
							logger.debug(str(o))

						for order in highest_solution:
							self.orders[order.get_id()] = order

						# generate a sequence id
						sequence_id = str(uuid.uuid4())
						self.sequences[sequence_id] = 0

						order_list = self.parallelize_orders(highest_solution)

						if not self.sequentialSequencesEnabled:
							# every order list in order_list must be of length 1 as we do not allow sequential orders
							for orders in order_list:
								if len(orders) > 1:
									logger.info("Invalid path because all orders cannot be executed in parallel")
									raise Exception("There are sequential orders")

						for orders in order_list:
							# if any sequence of orders cannot be started, then we must not execute the sequence
							if len(orders) > 0 and not orders[0].can_be_executed():
								logger.info("Cannot execute order due to %s insufficient balance (required %s) on %s.",
											orders[0].get_source_currency().get_code(),
											orders[0].get_source_amount(), orders[0].get_exchange())

								raise Exception("InsufficientBalance")

							# orders is an array of orders
							for i in range(0, len(orders) - 1):
								self.order_sequence[orders[i].get_id()] = orders[i + 1].get_id()
								orders[i].set_sequence_id(sequence_id)
								self.sequences[sequence_id] += 1

							self.order_sequence[orders[len(orders) - 1]] = None
							orders[len(orders) - 1].set_sequence_id(sequence_id)
							self.sequences[sequence_id] += 1

						for orders in order_list:
							if (len(orders) > 0) and self.trading_enabled:
								self.execute_order(orders[0])
						# orders[0].execute()

						# saving data to databse
						for i in range(1, len(highest_solution)):
							if highest_solution[i].is_deposit():
								self.db.add_transfer(highest_solution[i])

						self.db.add_sequence(sequence_id, order_list)

						self.lastSequenceStarted = time.time()
						self.currentSequences += 1
					except Exception as e:
						# cleaning up resources
						for order in highest_solution:
							self.orders[order.get_id()] = None
							self.order_sequence[order.get_id()] = None

						logger.error("Cleaning up resources")
						logger.error(e)

						self.started_order = False


	def on_order_update(self, order, status):
		"""
		Function executed when an order is updated
		"""
		logger.debug("Received an order update")

		if status == OrderStatus.COMPLETED:
			# order has been completed
			if order.get_id() in self.order_sequence:
				next_id = self.order_sequence[order.get_id()]

				if next_id is not None:
					next_order = self.orders[next_id]

					if self.trading_enabled:
						self.execute_order(next_order)
						#next_order.execute()

			sequence_id = order.get_sequence_id()
			with self.sequencesMutex:
				if sequence_id in self.sequences:
					self.sequences[sequence_id] -= 1

					if self.sequences[sequence_id] <= 0:
						self.started_order = False
						logger.info("Order sequence completed")
						self.currentSequences -= 1
						del self.sequences[sequence_id]

	def parallelize_orders(self, orders):
		"""
		Given an order list, it returns a list of order lists that can be executed at the same time
		:param orders: list of orders
		:return: list of order lists
		"""
		result = []

		# this will contain all the orders from the same exchange, that is all the orders between deposits
		tmp_orders = []

		i = 0
		while i < len(orders):
			while (i < len(orders)) and (not orders[i].is_deposit()):
				tmp_orders.append(orders[i])
				i += 1

			# we have reached the end of orders or found a deposit
			result.append(tmp_orders)
			tmp_orders = []
			i += 1

		# now, results is a list of order list
		# however, it might be possible that some orders inside an exchange can be executed in parallel
		tmp_result = result
		result = []

		for orders in tmp_result:
			tmp_orders = []

			for i in range(0, len(orders)):
				if orders[i].can_be_executed():
					if len(tmp_orders) > 0:
						result.append(tmp_orders)
					tmp_orders = [orders[i]]
				else:
					tmp_orders.append(orders[i])

			result.append(tmp_orders)

		return result

	def execute_order(self, order):
		"""
		Schedules the execution of an order
		:param order: order to be executed
		"""
		request = order.generate_request()
		request["id"] = 1 # task identifier
		self._tasks.put(request)

	def schedule_market_scan(self, market):
		"""
		Adds a market to the pending scan list, if it not there already
		"""
		with self._pendingScanMutex:
			if market.get_id() not in self._pendingScansList:
				self._pendingScansList[market.get_id()] = True
				self._pendingScans.put(market)

	def exit(self):
		"""
		Stops the Application
		"""
		# stopping the app
		self.stop = True

		#self._guiThread.join()

		# closing connection and exchange threads
		for exchange in self.exchanges:
			self.exchanges[exchange].stop()

		# force worker exit by adding a new task
		for worker in self._workers:
			self._tasks.put({ "id": 0 })

		self._pendingScans.put(None)

		# wait until all the workers have exited
		for worker in self._workers:
			worker.join()

	def debug(self):
		tmp = {"Binance": "BTCUSDT", "Poloniex": "USDT_BTC"}

		for k, v in tmp.items():
			logger.debug(k)
			logger.debug(self.exchanges[k]._markets[v])

			ask_price = self.exchanges[k]._markets[v].get_ask_price()
			bid_price = self.exchanges[k]._markets[v].get_bid_price()

			if ask_price is not None:
				logger.debug("A:" + str(ask_price.get_score()) + " - S: " + str(ask_price.get_item()))

			if bid_price is not None:
				logger.debug("B:" + str(bid_price.get_score()) + " - S: " + str(bid_price.get_item()))

	def test_order(self):
		# create a test order for BTC-USDT
		order = Order(
			Decimal("10000.00"),
			Decimal("0.001"),
			OrderSide.BUY,
			self.exchanges["Binance"]._markets["BTCUSDT"],
			maximum_amount=Decimal("0.003")
		)

		try:
			order.make_valid()
		except Exception:
			logger.error("Impossible to make order valid")

		#self.exchanges["Poloniex"].make_order(order)
		self.execute_order(order)

	def activate(self):
		self.trading_enabled = True

	def show_balances(self):
		for e in self.exchanges:
			logger.info("Balances %s", self.exchanges[e].get_name())

			for code in self.exchanges[e]._currencies:
				currency = self.exchanges[e]._currencies[code]
				logger.info("%s: %s", currency.get_code(), currency.get_balance().normalize())
