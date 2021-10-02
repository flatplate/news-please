import logging

import scrapy
from scrapy import Request


class Download(scrapy.Spider):
    name = "Download"
    start_urls = None

    log = None

    config = None
    helper = None

    def __init__(self, helper, url, config, ignore_regex, cookies, *args, **kwargs):
        self.log = logging.getLogger(__name__)

        self.config = config
        self.helper = helper

        if isinstance(url, list):
            self.start_urls = url
        else:
            self.start_urls = [url]

        self.cookies = cookies

        super(Download, self).__init__(*args, **kwargs)

    def start_requests(self):
        if not self.start_urls and hasattr(self, 'start_url'):
            raise AttributeError(
                "Crawling could not start: 'start_urls' not found "
                "or empty (but found 'start_url' attribute instead, "
                "did you miss an 's'?)")
        for url in self.start_urls:
            yield Request(url, dont_filter=True, cookies=self.cookies)

    def parse(self, response):
        """
        Passes the response to the pipeline.

        :param obj response: The scrapy response
        """
        if not self.helper.parse_crawler.content_type(response):
            return

        yield self.helper.parse_crawler.pass_to_pipeline(
            response,
            self.helper.url_extractor.get_allowed_domain(response.url)
        )

    @staticmethod
    def supports_site(url):
        """
        As long as the url exists, this crawler will work!

        Determines if this crawler works on the given url.

        :param str url: The url to test
        :return bool: Determines wether this crawler work on the given url
        """
        return True
