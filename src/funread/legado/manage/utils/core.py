from urllib.parse import urlparse

def url_to_hostname(url):
    return urlparse(url).hostname
