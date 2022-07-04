from ..Exchange import Exchange
from ..Market import Market
from ..Order import Order, OrderStatus, OrderSide
from ..rules.SizeRule import SizeRule
import requests
import websocket
import ssl
import logging
import threading
import json
import random
import time
import math
import hmac
import hashlib
from decimal import Decimal

logger = logging.getLogger("arbitrage_bot")

API_POLONIEX_PUBLIC = "https://poloniex.com/public"
API_POLONIEX_PRIVATE = "https://poloniex.com/tradingApi"
API_POLONIEX_WS = ""

class PoloniexExchange(Exchange):
	def __init__(self, api, secret, available_currencies={}, app=None):
		Exchange.__init__(self, "Poloniex", available_currencies, app=app)

		# saving private and public API keys
		self._api = api
		self._secret = secret

		# association between ids and markets
		self.market_to_id = {}
		self.id_to_market = {}

		# association between exchange currencies and currency codes
		self.id_to_currency = {}

		# orders and order ids
		self.order_id_lookup = {}
		self._orders = {}
		self._remainingBalance = {}

		# threads
		self._wsClient = None
		self._wsThread = None

		self._stop = False

	def initialize(self):
		logger.info("Initializing Poloniex exchange")

		# fetching all the available markets
		self.fetch_and_create_markets()
		self.fetch_fees()

		# exchange is ready at this point
		self.set_ready(True)

		# connecting to websocket server
		self.create_ws_stream()

		self.fetch_balances()

	def fetch_and_create_markets(self):
		markets = self.fetch("?command=returnTicker")

		for market in markets:
			tmp = market.split("_")
			base_currency_name = tmp[1]
			quote_currency_name = tmp[0]

			if base_currency_name in self._availableCurrencies:
				if quote_currency_name in self._availableCurrencies:
					base_currency_name = self._availableCurrencies[base_currency_name]
					quote_currency_name = self._availableCurrencies[quote_currency_name]

					base_curency = self.find_or_create_exchange_currency(base_currency_name)
					quote_currency = self.find_or_create_exchange_currency(quote_currency_name)

					# create exchange markets
					self._markets[market] = Market(base_curency, quote_currency, market, self)
					self._markets[market].set_taker_fees(Decimal(0.125) / Decimal(100))
					self._markets[market].set_maker_fees(Decimal(0.125) / Decimal(100))

					# step must be a multiple of 0.00000001
					self._markets[market].add_rule(
						SizeRule(
							Decimal(0),
							Decimal(0),
							Decimal("0.00000001")
						)
					)

					print(quote_currency, base_curency)

					base_curency.add_neighbour_currency(quote_currency, self._markets[market])

					# save market id and name
					self.market_to_id[market] = markets[market]["id"]
					self.id_to_market[markets[market]["id"]] = market

	def create_ws_stream(self):
		# connecting to websockets
		def on_open(ws):
			logger.debug("Poloniex Web Socket connection established")

			# subscribing to channels
			for market in self._markets:
				tmp = {
					"command": "subscribe",
					"channel": market
				}
				logger.debug("subscribing to %s" % market, tmp)
				self._wsClient.send(json.dumps(tmp))

			# subscribing to account updates
			body = {
				"nonce": math.floor(time.time() * 1000)
			}

			signature = self._sign(body)

			account_subscription = {
				"command": "subscribe",
				"channel": "1000",
				"key": self._api,
				"payload": signature[1],
				"sign": signature[0]
			}

			self._wsClient.send(json.dumps(account_subscription))
			logger.debug("Sending account subscription", account_subscription)

		def on_close(ws, error):
			logger.debug("Poloniex Web Socket connection closed")
			logger.debug(error)

		def on_msg(ws, msg):
			try:
				msg = json.loads(msg)

				channel_id = msg[0]

				if channel_id == 1000:
					if len(msg) == 2 and msg[1] == 1:
						logger.info("Subscribed to Account Updates")
						return

					logger.debug(msg)

					# account update message
					updates = msg[2]

					for update in updates:
						update_type = update[0]

						if update_type == "o":  # an order update
							if update[4] is not None:
								order_id = int(update[4])
							else:
								# skip this update, it was not made by the bot
								continue

							pending_amount = Decimal(update[2])
							if pending_amount > Decimal(0):
								# this order is not completely filled
								continue

							logger.debug("Received info from order %d", order_id)

							if order_id in self.order_id_lookup:
								# we have created this order
								order_status = update[3]

								if order_status == "f" or order_status == "s":
									status = OrderStatus.COMPLETED
								else:
									status = OrderStatus.REJECTED

								# notify the order update
								order = self._orders[self.order_id_lookup[order_id]]
								self.invoke_listener("orderUpdate", order, status)

								# remove the order if it is completed or rejected
								if status == OrderStatus.COMPLETED or status == OrderStatus.REJECTED:
									logger.debug("Removing local order %s", self.order_id_lookup[order_id])

									del self._orders[self.order_id_lookup[order_id]]
									del self.order_id_lookup[order_id]
						elif update_type == "b": # a balance update
							currency_id = update[1]

							logger.debug("Received a balance update")
							logger.debug(update)

							if update[2] == "e": # only exchange balance updates
								# get exchange currency, if exists
								if currency_id in self.id_to_currency:
									logger.debug("updating %s balance", self.id_to_currency[currency_id])
									self._currencies[self.id_to_currency[currency_id]].add_balance(Decimal(update[3]))
						elif update_type == "t": # a trade update
							order_id = update[9]

							logger.debug("Trade update")
							logger.debug(update)

							if order_id in self._remainingBalance:
								# substract the trade amount
								amount = Decimal(update[3])
								self._remainingBalance[order_id] -= amount

								logger.debug("Order found in remaining balance (left %s)", str(self._remainingBalance[order_id]))

								if self._remainingBalance[order_id] <= 0:
									logger.debug("Order completed by trade total")
									# order is completed
									del self._remainingBalance[order_id]

									order = self._orders[self.order_id_lookup[order_id]]
									self.invoke_listener("orderUpdate", order, OrderStatus.COMPLETED)

									# remove order
									del self._orders[self.order_id_lookup[order_id]]
									del self.order_id_lookup[order_id]
						else:
							logger.debug(update)
				elif channel_id == 1010:
					# just a heartbeat message, ignore it
					pass
				else:
					# check if this message is book order update
					if channel_id in self.id_to_market:
						market = self._markets[self.id_to_market[channel_id]]

						for update in msg[2]:
							type = update[0]

							if type == "i":
								# first time receiving information for this market
								market.reset_prices()

								order_book = update[1]["orderBook"]
								asks = order_book[0]
								bids = order_book[1]

								for ask_price in asks:
									market.update_ask_price(Decimal(ask_price), Decimal(asks[ask_price]))

								for bid_price in bids:
									market.update_bid_price(Decimal(bid_price), Decimal(bids[bid_price]))

								logger.info("Poloniex resetting prices")

								# market has been updated with the latest prices
								self._app.schedule_market_scan(market)
							elif type == "o":
								# just a normal update
								mode = update[1]
								price = Decimal(update[2])
								quantity = Decimal(update[3])

								if mode == 1:
									# bid price
									market.update_bid_price(price, quantity)
								elif mode == 0:
									# ask price
									market.update_ask_price(price, quantity)
							elif type == "t":
								# trade update, ignore it
								pass

						# market update is completed
						self._app.schedule_market_scan(market)
					else:
						logger.debug("channel=%d" % channel_id)
						logger.debug(msg)
			except json.JSONDecodeError:
				logger.error("Couldn't decode Poloniex WS message")
				logger.error(msg)
			except Exception as e:
				logger.error("Unknow error", e)

		self._wsClient = websocket.WebSocketApp(
			"wss://api2.poloniex.com",
			on_open=on_open,
			on_close=on_close,
			on_message=on_msg
		)

		def run():
			logger.debug("Poloniex WS thread started")

			while not self._stop:
				self._wsClient.run_forever(
					ping_interval=60,
					sslopt={"cert_reqs": ssl.CERT_NONE}
				)

				time.sleep(1)

		self._wsThread = threading.Thread(target=run, name="PoloniexWS")
		self._wsThread.start()

	def fetch_balances(self):
		# before fetching balances, we must load the association between currency code and currency id
		try:
			request = self.fetch(
				"?command=returnCurrencies",
				method="GET"
			)
		except Exception:
			logger.error("Could not load Poloniex currencies")
			return

		for code in request:
			currency = request[code]

			if code in self._currencies:
				currency_id = currency["id"]
				self.id_to_currency[currency_id] = code

		# fetching initial free balances
		body = {
			"command": "returnBalances"
			# timestamp will be added on request signing
		}

		try:
			request = self.fetch(
				"",
				method="POST",
				authentication=True,
				signature=True,
				parameters=body
			)
		except Exception as e:
			logger.error(e)
			logger.error("Could not fetch Poloniex user balances")
			return

		for currency in request:
			if currency in self._currencies:
				self._currencies[currency].set_balance(Decimal(request[currency]))

		logger.info("Successfully fetched Poloniex user balances")

	def fetch_fees(self):
		try:
			body = {
				"command": "returnFeeInfo"
				# nonce will be added on signing
			}

			request = self.fetch(
				"",
				method="POST",
				authentication=True,
				signature=True,
				parameters=body
			)

			if ("makerFee" not in request) or ("takerFee" not in request):
				raise Exception()
		except Exception:
			self.error("Could not fetch Poloniex fees")
			return

		for market in self._markets:
			self._markets[market].set_taker_fees(Decimal(request["takerFee"]))
			self._markets[market].set_maker_fees(Decimal(request["makerFee"]))

		logger.info("Poloniex fees fetched successfully")

	def stop(self):
		logger.info("Stopping Poloniex Exchange")
		self._stop = True

		if self._wsClient is not None:
			logger.debug("Closing WS and waiting for thread exit")
			self._wsClient.close()
			self._wsThread.join()

	def _sign(self, parameters={}):
		"""
		Creates a signature using Poloniex algorithm
		:return: a tuple with the signature and the query string
		"""
		params = []
		for k, v in parameters.items():
			params.append(str(k) + "=" + str(v))

		qs = "&".join(params)

		# sha512 signature with private key
		signature = hmac.new(
			bytes(self._secret, encoding="utf-8"),
			bytes(qs, encoding="utf-8"),
			digestmod=hashlib.sha512
		).hexdigest()

		return (signature, qs)

	def fetch(self, endpoint, method="GET", authentication=False, signature=False, headers={}, parameters=None):
		if authentication is False:
			# it is a public api call
			method = "GET"
			base_url = API_POLONIEX_PUBLIC
		elif authentication is True:
			# it is a private request
			method = "POST"
			base_url = API_POLONIEX_PRIVATE

			# adding timestamp
			if "nonce" not in parameters:
				parameters["nonce"] = math.floor(time.time() * 1000)

			signature = self._sign(parameters)

			headers["Key"] = self._api
			headers["Sign"] = signature[0]

		return requests.request(
			method,
			base_url + endpoint,
			data=parameters,
			headers=headers
		).json()

	def make_order(self, order, test=False):
		# choose the right endpoint depending the order type
		if order.get_side() == OrderSide.BUY:
			command = "buy"
		else:
			command = "sell"

		# generate an "unique" numeric order id
		new_id = str(random.randint(0, 2**63))

		body = {
			"command": command,
			"currencyPair": order.get_symbol(),
			"rate": str(order.get_price()),
			"amount": str(order.get_size()),
			"clientOrderId": new_id
		}

		# create an association between both ids
		self.order_id_lookup[new_id] = order.get_id()
		self._orders[order.get_id()] = order
		self._remainingBalance[new_id] = order.get_size()

		logger.debug("Created order %s", new_id)

		request = self.fetch(
			"",
			method="POST",
			authentication=True,
			signature=True,
			parameters=body
		)

		if "error" in request:
			logger.error("Poloniex order rejected")
			logger.error(request)
			return False

		return True

	def generate_order_request(self, order, test=False):
		request = {
			"headers": {
				"Key": self._api,
				# add signature (Sign)
			},
			"method": "POST",
			"url": API_POLONIEX_PRIVATE,
			# body
		}

		# generate order id
		new_id = str(random.randint(0, 2 ** 63))

		body = {
			"command": "buy" if order.get_side() == OrderSide.BUY else "sell",
			"currencyPair": order.get_symbol(),
			"rate": str(order.get_price()),
			"amount": str(order.get_size()),
			"clientOrderId": new_id,
			"nonce": math.floor(time.time() * 1000)
		}

		request["parameters"] = body

		# signing the request
		request["headers"]["Sign"] = self._sign(body)[0]

		# add this order id to the lookup table
		self.order_id_lookup[new_id] = order.get_id()
		self._orders[order.get_id()] = order
		self._remainingBalance[new_id] = order.get_size()

		return request