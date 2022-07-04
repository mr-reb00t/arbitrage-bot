class ScoreItem(object):
	"""
	Allows to create relations between a value and an item
	This enables to order items by score value in an ordered array
	"""
	def __init__(self, score, item, inversed=False):
		"""
		:param score: value assigned to this item
		:param item:
		"""
		if inversed:
			score = -score

		self.inversed = inversed
		self.score = score
		self.item = item

	def __lt__(self, other):
		return self.score < other.score

	def __gt__(self, other):
		return self.score > other.score

	def __eq__(self, other):
		return self.score == other.score

	def __str__(self):
		return str(self.item)

	def get_score(self):
		if self.inversed:
			return -self.score
		else:
			return self.score

	def get_item(self):
		return self.item