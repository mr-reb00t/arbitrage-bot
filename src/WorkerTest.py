import logging
from multiprocessing import Process, Queue
import time


logger = logging.getLogger("arbitrage_bot")

# print logs to console
console_handler = logging.StreamHandler()

# formatting the log output
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(threadName)s: %(message)s")
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

def work(id):
	logger.info("Hello from another process (%d)" % id)
	i = 0;
	while i < 100000000:
		i += 1
	pass


if __name__ == "__main__":
	# create 4 workers
	n = 4
	workers = []

	for i in range(0, 4):
		workers.append(
			Process(target=work, args=(i,), name="Worker " + str(i))
		)

	for worker in workers:
		worker.start()

	logger.info("Main process here")

	for worker in workers:
		worker.join()