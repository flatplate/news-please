import logging

from newsplease.config import CrawlerConfig
from newsplease.pipeline.extractor import article_extractor


class ArticleMasterExtractor(object):
    """
    Parses the HTML response and extracts title, description,
    text, image and meta data of an article.
    """

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.cfg = CrawlerConfig.get_instance()
        self.extractor_list = self.cfg.section("ArticleMasterExtractor")[
            "extractors"]

        self.extractor = article_extractor.Extractor(self.extractor_list)

    def process_item(self, item, spider):
        return self.extractor.extract(item)