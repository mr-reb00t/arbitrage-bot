from multiprocessing import Process, Queue
import requests
import logging

logger = logging.getLogger("arbitrage_bot")


class Worker(Process):
	def __init__(self, id, queue):
		Process.__init__(self)

		self._id = id

		# task queue
		self._queue = queue

	def run(self):
		logger.info("Worker %s has started" % (self._id))

		run = True
		while run:
			task = self._queue.get()

			if task["id"] == 0:
				# shutdown ask
				logger.info("Worker %d exiting" % (self._id))
				run = False
			elif task["id"] == 1:
				# request task
				# a request task must have the following field in the task object:
				# - url: URL of the HTTP request
				# - method: the HTTP method to perform the HTTP request
				# - headers: a dictionary (key-value)
				# - parameters: a dictionary (key-value)
				try:
					response = requests.request(
						method=task["method"],
						url=task["url"],
						headers=task["headers"],
						data=task["parameters"]
					).json()

					logger.info("Successfully sent order request")
					logger.debug(response)
					logger.debug(task)
				except Exception as e:
					# it might have gone something wrong, but we do not care
					# however, thread must be able to continue processing new tasks
					logger.error("Something went wrong making a request")
					logger.debug(task)
					logger.error(e)

		logger.info("Worker %d has exited" % (self._id))