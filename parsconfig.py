import yaml

class Config(object):
	"""docstring for Congig"""
	def __init__(self, file_name):		
		self.config = yaml.load(open(file_name))
		self.main = self.config["damain"]
		self.name_archive = self.config["damain"]["archive_storage"]
		self.archive = self.config["{}".format(self.name_archive)]
		self.name_compress_archive = self.config["damain"]["compress_archive_storage"]
		self.compress_archive = self.config["{}".format(self.name_compress_archive)]
		self.name_storage = self.config["damain"]["chunk_storage"]
		self.storage = self.config["{}".format(self.name_storage)]
		self.celery = self.config["celery"]

		

