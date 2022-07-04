from App import App
import logging
from logging.handlers import TimedRotatingFileHandler
import time
import os

logsDir = "logs/"

# create the directories if not exists
if not os.path.exists(logsDir):
    os.mkdir(logsDir)

logger = logging.getLogger("arbitrage_bot")

# save logs to a file, one file for each day
#file_handler = logging.FileHandler(time.strftime(logsDir+yearMonthDir+"/arbitrage_%Y-%m-%d.log"))
file_handler = TimedRotatingFileHandler(logsDir+"arbitrage.log", when="midnight", interval=1)
file_handler.suffix = "%Y-%m-%d"

# print logs to console
console_handler = logging.StreamHandler()

# formatting the log output
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(threadName)s: %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
	# initializing logger instance

	app = None
	try:
		app = App()

		# check if trading is enabled by default
		if "TRADING" in os.environ:
			if os.environ["TRADING"] == "1":
				logger.info("Enabling trading by default (TRADING=1)")
				app.activate()

		if not app.trading_enabled:
			logger.info("Trading is not enabled")

		run = True
		while run:
			command = input()

			if command == "exit":
				run = False
			elif command == "show":
				app.debug()
			elif command == "order":
				app.test_order()
			elif command == "activate":
				logger.info("Trading enabled")
				app.activate()
			elif command == "balances":
				app.show_balances()

		app.exit()
	except KeyboardInterrupt:
		logger.info("App is closing")
		app.exit()
