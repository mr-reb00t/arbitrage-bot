from ..Exchange import Exchange
from ..Market import Market
from ..rules.SizeRule import SizeRule
from ..rules.ValueRule import ValueRule
from ..Order import OrderSide, OrderStatus
import requests
import websocket
import ssl
import threading
import json
import logging
import hmac
import hashlib
import time
from decimal import Decimal
from math import floor

API_BINANCE = "https://api.binance.com"
logger = logging.getLogger("arbitrage_bot")


class BinanceExchange(Exchange):
	def __init__(self, api, secret, available_currencies={}, app=None):
		Exchange.__init__(self, "Binance", available_currencies, app=app)

		self._api = api
		self._secret = secret

		# websocket threads and clients
		self._marketStreamThread = None
		self._marketStreamClient = None

		# account updates notifications
		self._listenKey = None
		self._lastKeyRequest = time.time()
		self._accountUpdatesThread = None
		self._accountUpdatesClient = None

		# keep-alive threads
		self._keepAliveSleep = threading.Lock()
		self._keepAliveThread = None
		self._lastWSRestart = time.time()

		# current orders
		self._orders = {}

	def initialize(self):
		logger.info("Initializing Binance exchange")

		# creating available markets
		self.fetch_and_create_markets()

		# fetching market fees
		self.fetch_market_fees()

		# exchange is ready at this point
		self.set_ready(True)

		# initialize order book update stream
		self.create_market_stream()

		# initialize account update stream
		self.create_account_stream()

		# fetch initial account balances
		self.fetch_account_balances()

		# create keep alive threads
		self.create_keep_alive_thread()

	def fetch_and_create_markets(self):
		"""
		This method retrieves all available markets from the exchange and
		create the market's structure
		"""
		exchangeInfo = self.fetch(
			"/api/v3/exchangeInfo"
		)

		# create market from data obtained
		for market in exchangeInfo["symbols"]:
			if market["baseAsset"] in self._availableCurrencies and market["quoteAsset"] in self._availableCurrencies:
				base = self.find_or_create_exchange_currency(self._availableCurrencies[market["baseAsset"]])
				quote = self.find_or_create_exchange_currency(self._availableCurrencies[market["quoteAsset"]])

				exchange_market = Market(
					base,
					quote,
					market["symbol"],
					self
				)

				base.add_neighbour_currency(quote, exchange_market)

				for rule in market["filters"]:
					if rule["filterType"] == "LOT_SIZE":
						# this is a SizeRule
						exchange_market.add_rule(
							SizeRule(
								Decimal(rule["minQty"]),
								Decimal(rule["maxQty"]),
								Decimal(rule["stepSize"])
							)
						)
					elif rule["filterType"] == "MIN_NOTIONAL":
						# this a ValueRule
						exchange_market.add_rule(
							ValueRule(Decimal(rule["minNotional"]))
						)

				self._markets[market["symbol"]] = exchange_market

	def fetch_market_fees(self):
		"""
		This method fetches and sets market fees
		"""
		# fetch fee data
		feeData = self.fetch(
			"/wapi/v3/tradeFee.html",
			authentication=True,
			signature=True
		)

		try:
			if "tradeFee" in feeData:
				for symbol in feeData["tradeFee"]:
					if symbol["symbol"] in self._markets:
						self._markets[symbol["symbol"]].set_taker_fees(Decimal(symbol["taker"]))
						self._markets[symbol["symbol"]].set_maker_fees(Decimal(symbol["maker"]))

				logger.info("Fee data loaded")
			else:
				logger.error("Could not fetch fee data")
		except:
			logger.error("Invalid fee data response")

	def create_market_stream(self):
		"""
		Creates the WebSocket client and thread for order book updates
		Once created, initializes the connection
		"""
		# once we have created all available markets, let's start opening websocket connections
		qs = "/".join(map(lambda symbol: symbol.lower() + "@depth5@100ms", self._markets.keys()))

		def on_open(ws):
			logger.debug("Binance Market Stream is connected")

		def on_close(ws):
			logger.info("Binance Market Stream closed")

		def on_msg(ws, msg):
			try:
				msg = json.loads(msg)
				data = msg["data"]

				# parse message symbol
				stream_name = msg["stream"]
				market_name = stream_name[:stream_name.index("@")].upper()

				if market_name in self._markets:
					market = self._markets[market_name]

					market.reset_prices()

					# update ask price
					for ask in data["asks"]:
						market.update_ask_price(
							Decimal(ask[0]),
							Decimal(ask[1])
						)

					# update bid price
					for bid in data["bids"]:
						market.update_bid_price(
							Decimal(bid[0]),
							Decimal(bid[1])
						)

					# market has been updated with the latest prices
					self._app.schedule_market_scan(market)

			except json.JSONDecodeError:
				logger.error("Invalid Binace WS message", msg)

		def on_ping(ws):
			logger.info("PING RECEIVED")

		self._marketStreamClient = websocket.WebSocketApp(
			"wss://stream.binance.com:9443/stream?streams=" + qs,
			on_open=on_open,
			on_close=on_close,
			on_message=on_msg,
			on_ping=on_ping
		)

		def run():
			logger.debug("Binance Market Stream client is running")

			while not self._stop:
				self._marketStreamClient.run_forever(
					ping_interval=60,
					sslopt={"cert_reqs": ssl.CERT_NONE}
				)

				time.sleep(1)

		self._marketStreamThread = threading.Thread(target=run, name="BinanceWS")
		self._marketStreamThread.start()

	def create_account_stream(self):
		# create a listening key for the current API key
		try:
			key_request = self.fetch(
				"/api/v3/userDataStream",
				method="POST",
				authentication=True,
				signature=False
			)

			logger.debug("Response is " + str(key_request))

			if "listenKey" in key_request:
				self._listenKey = key_request["listenKey"]
				logger.debug("Binance Account Listen Key fetched successfully")
			else:
				raise Exception()
		except Exception as e:
			logger.error("Failed to create a listen key for this account")
			self._listenKey = None

		# creating WS client and thread
		def on_open(ws):
			logger.info("Listening for account updates on Binance")

		def on_close(ws):
			logger.info("Account updates stream closed")

		def on_msg(ws, msg):
			try:
				msg = json.loads(msg)
				msg = msg["data"]

				logger.debug(msg)

				if "e" in msg:
					if msg["e"] == "executionReport":
						# check if we must notify this order update
						order_id = msg["c"]

						if msg["X"] == "CANCELED" and len(msg["C"]) > 0:
							order_id = msg["C"]

						if order_id in self._orders:
							logger.debug("Received order update from %s", order_id)
							if msg["X"] == "NEW" or msg["X"] == "PARTIALLY_FILLED":
								status = OrderStatus.PENDING
							elif msg["X"] == "FILLED":
								status = OrderStatus.COMPLETED
							else:
								status = OrderStatus.REJECTED

							self.invoke_listener("orderUpdate", self._orders[order_id], status)

							# if the order is marked as completed or rejected, we can remove it
							if status == OrderStatus.COMPLETED or status == OrderStatus.REJECTED:
								logger.debug("Removing local order from Binance (%s)", order_id)
								del self._orders[order_id]
					elif msg["e"] == "outboundAccountPosition":
						# there has been a balance update on any currency
						for balance in msg["B"]:
							if balance["a"] in self._currencies:
								logger.debug("Updating balance %s", balance["a"])
								self._currencies[balance["a"]].set_balance(Decimal(balance["f"]))
			except json.JSONDecodeError:
				logger.error("Invalid JSON message", msg)
			except KeyError:
				logger.error("Could not find data payload")

		def run():
			while not self._stop:
				self._accountUpdatesClient.run_forever(
					ping_interval=300, # ping every 5 minutes
					sslopt={"cert_reqs": ssl.CERT_NONE}
				)

				time.sleep(1)

		self._accountUpdatesClient = websocket.WebSocketApp(
			"wss://stream.binance.com:9443/stream?streams=" + self._listenKey,
			on_open=on_open,
			on_close=on_close,
			on_message=on_msg
		)

		self._accountUpdatesThread = threading.Thread(target=run, name="BinanceAccount")
		self._accountUpdatesThread.start()

	def fetch_account_balances(self):
		# fetch account balances
		try:
			request = self.fetch(
				"/api/v3/account",
				method="GET",
				authentication=True,
				signature=True
			)

			if "code" in request:
				raise Exception()
		except Exception:
			logger.error("Could not fetch account balances")
			return

		# we got a valid response
		if "balances" in request:
			for balance in request["balances"]:
				if balance["asset"] in self._currencies:
					self._currencies[balance["asset"]].set_balance(Decimal(balance["free"]))

			logger.info("Successfully fetched Binance account balances")
		else:
			logger.error("Could not update Binance account balance data")

	def create_keep_alive_thread(self):
		def run():
			while not self._stop:
				now = time.time()

				# check if we have to renew the listen key
				if (now - self._lastKeyRequest) > (30 * 60):
					logger.debug("Renewing Binance Listen Key")

					request = self.fetch(
						"/api/v3/userDataStream",
						method="PUT",
						authentication=True,
						signature=False,
						parameters={ "listenKey": self._listenKey }
					)

					if "code" in request:
						logger.error("Could not renew current listen key")

						if request["code"] == -1125 or request["code"] == -3038:
							# listen key is invalid, create a new listen key
							try:
								key_request = self.fetch(
									"/api/v3/userDataStream",
									method="POST",
									authentication=True,
									signature=False
								)

								if "listenKey" in key_request:
									self._listenKey = key_request["listenKey"]
							except Exception:
								logger.error("Could not obtain a new Binance Listen Key")
					else:
						logger.info("Current Binance Listen Key has been renewed for 60 minutes more")
						self._lastKeyRequest = now

				# check if WS have been restarted in the last 12 hours
				if (now - self._lastWSRestart) > (12 * 60 * 60):
					# we only have to close their connections
					# each WS will make sure to restart its connection
					logger.debug("Restarting Binance WS to avoid 24h disconnection")

					if self._marketStreamClient is not None:
						self._marketStreamClient.close()

					if self._accountUpdatesClient is not None:
						self._accountUpdatesClient.close()

					self._lastWSRestart = now

				# wait a few seconds to check again that everything is right
				self._keepAliveSleep.acquire(timeout=10)

		self._keepAliveThread = threading.Thread(target=run, name="BinanceKA")
		self._keepAliveThread.start()


	def fetch(self, endpoint, method="GET", authentication=False, signature=False, headers={}, parameters=None):
		qs = ""

		if authentication:
			headers["X-MBX-APIKEY"] = self._api

		if signature:
			if parameters is None:
				parameters = {}

			if "timestamp" not in parameters:
				parameters["timestamp"] = floor(time.time() * 1000)

			params = []
			for k, v in parameters.items():
				params.append(str(k) + "=" + str(v))

			qs = "&".join(params)
			signature = hmac.new(
				bytes(self._secret, encoding="utf-8"),
				bytes(qs, encoding="utf-8"),
				digestmod=hashlib.sha256
			).hexdigest()

			parameters["signature"] = signature

			print(parameters)
			params.append("signature=" + signature)
			qs = "&".join(params)

		print(qs)

		if method == "POST" or method == "PUT" or method == "DELETE":
			final_url = API_BINANCE + endpoint
		else:
			parameters = None
			final_url = API_BINANCE + endpoint + "?" + qs

		return requests.request(
			method,
			final_url,
			headers=headers,
			params=parameters
		).json()

	def stop(self):
		self._stop = True

		if self._marketStreamClient is not None:
			self._marketStreamClient.close()
			logger.debug("Waiting for WS Market Thread to exit")

			if self._marketStreamThread is not None:
				self._marketStreamThread.join()

		if self._keepAliveThread is not None:
			# force thread exit
			self._keepAliveSleep.release()

			# and wait its exit
			self._keepAliveThread.join()

			logger.debug("Binance Keep Alive thread has exited")

		if self._listenKey is not None:
			# deactivate the current listen key
			response = self.fetch(
				"/api/v3/userDataStream",
				method="DELETE",
				parameters={ "listenKey": self._listenKey },
				authentication=True,
				signature=False
			)

			logger.debug(response)

			self._listenKey = None

			# stop WS account connection, if running
			if self._accountUpdatesClient is not None:
				self._accountUpdatesClient.close()
				self._accountUpdatesClient = None

				if self._accountUpdatesThread is not None:
					logger.debug("Waiting for WS Account Thread to exit")
					self._accountUpdatesThread.join()
					self._accountUpdatesThread = None

	def make_order(self, order, test=False):
		# we might want to simulate the order to check that everything is right
		if test:
			endpoint = "/api/v3/order/test"
		else:
			endpoint = "/api/v3/order"

		side = "BUY" if order.get_side() == OrderSide.BUY else "SELL"

		# order is valid, we only have to post it into the market
		logger.info("Sending order %s (%s) to market", order, order.get_id())
		body = {
			"symbol": order.get_symbol(),
			"side": side,
			"type": "LIMIT",
			"quantity": str(order.get_size().normalize()),
			"price": str(order.get_price()),
			"newClientOrderId": order.get_id(),
			"timeInForce": "GTC" # good til canceled
		}

		# add the order to the internal list
		self._orders[order.get_id()] = order


		# as this request must be signed, timestamp field will be added by the
		# fetch method on request signing
		request = self.fetch(
			endpoint,
			method="POST",
			parameters=body,
			authentication=True,
			signature=True
		)

		logger.debug(request)

		if "code" in request:
			# when an order is rejected, a standard error message is sent
			logger.error("Binance order rejected.")
			return False


		return True

	def generate_order_request(self, order, test=False):
		request = {
			"headers": {
				"X-MBX-APIKEY": self._api
			},
			"method": "POST",
			"url": API_BINANCE + "/api/v3/order",
		}

		# generating the body of the request
		body = {
			"symbol": order.get_symbol(),
			"side": "BUY" if order.get_side() == OrderSide.BUY else "SELL",
			"type": "LIMIT",
			"quantity": str(order.get_size().normalize()),
			"price": str(order.get_price()),
			"newClientOrderId": order.get_id(),
			"timeInForce": "GTC",  # good til canceled
			"timestamp": floor(time.time() * 1000)
		}

		request["parameters"] = body

		# generating the signature
		body["signature"] = self._sign_parameters(body)

		# add the order to the internal list
		self._orders[order.get_id()] = order

		return request


	def _sign_parameters(self, parameters):
		"""
		Creates a signatures a from a parameter dictionary (key: value)
		:param parameters: dictionary
		:return: signature string
		"""
		params = []

		for key, value in parameters.items():
			params.append(str(key) + "=" + str(value))

		qs = "&".join(params)
		signature = hmac.new(
			bytes(self._secret, encoding="utf-8"),
			bytes(qs, encoding="utf-8"),
			digestmod=hashlib.sha256
		).hexdigest()

		return signature
