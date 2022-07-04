import abc


class Rule(object):
	@abc.abstractmethod
	def make_valid(self, order):
		"""
		Tries to make an order compliant, if not already, with this market rule
		:return: whether this rule has made changes or not
		"""
		pass