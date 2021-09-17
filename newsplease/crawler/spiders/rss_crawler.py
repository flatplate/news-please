from newsplease.helper_classes.url_extractor import UrlExtractor
import xml.etree.ElementTree as ET

try:
    import urllib2
except ImportError:
    import urllib.request as urllib2
import logging
import re

import scrapy

# to improve performance, regex statements are compiled only once per module
re_rss = re.compile(
    r'(<link[^>]*href[^>]*type ?= ?"application\/rss\+xml"|' +
    r'<link[^>]*type ?= ?"application\/rss\+xml"[^>]*href)'
)


class RssCrawler(scrapy.Spider):
    name = "RssCrawler"
    ignored_allowed_domains = None
    start_urls = None
    original_url = None

    log = None

    config = None
    helper = None

    def __init__(self, helper, url, config, ignore_regex, *args, **kwargs):
        self.log = logging.getLogger(__name__)

        self.config = config
        self.helper = helper

        self.original_url = url

        if url.startswith("https://news.google.com/rss"):
            self.ignored_allowed_domain = url.split("allinurl:")[-1]
        else:
            self.ignored_allowed_domain = self.helper.url_extractor \
                .get_allowed_domain(url)
        self.start_urls = [url]
        print("Allowed Domain", self.ignored_allowed_domain)

        super(RssCrawler, self).__init__(*args, **kwargs)

    def parse(self, response):
        """
        Extracts the Rss Feed and initiates crawling it.

        :param obj response: The scrapy response
        """
        print("RESPONSE URL", response.url)
        if any(True for _ in response.xpath("//rss")):
            print("True")
            for x in self.rss_parse(response):
                yield x
        else:
            req = scrapy.Request(self.helper.url_extractor.get_rss_url(response),
                             callback=self.rss_parse)
            req.cookies['wp_devicetype'] = "0"
            req.cookies['wp_gdpr'] = "1|1"
            yield req

    def rss_parse(self, response):
        """
        Extracts all article links and initiates crawling them.

        :param obj response: The scrapy response
        """
        for item in response.xpath('//item'):
            for url in item.xpath('link/text()').extract():
                req = scrapy.Request(url, lambda resp: self.article_parse(
                    resp, item.xpath('title/text()').extract()[0]))
                # TODO This is just a very ugly hack to get through
                # washington post's gdpr page. Implement adding custom
                # headers per site in the config and remove this.
                req.cookies['wp_devicetype'] = "0"
                req.cookies['wp_gdpr'] = "1|1"
                yield req

    def article_parse(self, response, rss_title=None):
        """
        Checks any given response on being an article and if positiv,
        passes the response to the pipeline.

        :param obj response: The scrapy response
        :param str rss_title: Title extracted from the rss feed
        """
        if not self.helper.parse_crawler.content_type(response):
            return

        yield self.helper.parse_crawler.pass_to_pipeline_if_article(
            response, self.ignored_allowed_domain, self.original_url,
            rss_title)

    @staticmethod
    def only_extracts_articles():
        """
        Meta-Method, so if the heuristic "crawler_contains_only_article_alikes"
        is called, the heuristic will return True on this crawler.
        """
        return True

    @staticmethod
    def supports_site(url):
        """
        Rss Crawler are supported if by every site containing an rss feed.

        Determines if this crawler works on the given url.

        :param str url: The url to test
        :return bool: Determines wether this crawler work on the given url
        """

        # Follow redirects
        opener = urllib2.build_opener(urllib2.HTTPRedirectHandler)
        url = UrlExtractor.url_to_request_with_agent(url)
        redirect = opener.open(url).url
        redirect = UrlExtractor.url_to_request_with_agent(redirect)
        response = urllib2.urlopen(redirect).read()
        txt = response.decode('utf-8')

        # Check if a standard rss feed exists
        if re.search(re_rss, txt) is not None:
                return True

        try:
            root = ET.fromstring(txt)
            if root.tag == 'rss' or root.find('rss'):
                return True
            return False
        except Exception as e:
            logging.error(e)
            return False
