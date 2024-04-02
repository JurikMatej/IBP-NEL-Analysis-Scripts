from collections import defaultdict, namedtuple

RegistryHeaders = namedtuple('RegistryHeaders', ['nel_header', 'rt_header'])


class CrawledDomainNelRegistry(object):

    def __init__(self):
        self._registry = defaultdict(list[dict])

    def insert(self, domain_name, url, nel_header, rt_header):
        self._registry[domain_name].append({
            url: RegistryHeaders(nel_header, rt_header)
        })

    def get_content(self):
        return self._registry

    def get_domain_crawled_urls(self, domain_name):
        return [url for entry in self._registry[domain_name] for url in entry]
