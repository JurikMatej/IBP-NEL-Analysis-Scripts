class CrawledDomainIndexer(object):
    def __init__(self, starting_index):
        self.index = starting_index

    def next_index(self):
        self.index += 1

    def get_index(self):
        return self.index

    def set_index(self, idx):
        self.index = idx
