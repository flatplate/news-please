from .abstract_extractor import AbstractExtractor
from ..article_candidate import ArticleCandidate
from bs4 import BeautifulSoup
from bs4.element import Tag
import json
from datetime import datetime


class LdjsonExtractor(AbstractExtractor):
    """
    Extractor that uses the ld+json data inside a web page
    to extract metadata.
    """

    def __init__(self):
        self.name = "ldjson"

    def extract(self, item):
        """Executes all implemented functions on the given article and returns an
        object containing the recovered data.

        :param item: A NewscrawlerItem to parse.
        :return: ArticleCandidate containing the recovered article data.
        """
        article_candidate = ArticleCandidate()
        article_candidate.extractor = self._name()

        soup = BeautifulSoup(item['spider_response'].body)
        ldjson_candidates = soup.select('script[type="application/ld+json"]')

        if not ldjson_candidates:
            return article_candidate

        parsed_ldjson = [self._map_ldjson(ldjson_tag) for ldjson_tag in ldjson_candidates]
        filtered_ldjson = [ldjson for ldjson in parsed_ldjson if "@type" in ldjson and ldjson["@type"] == "NewsArticle"]

        if not filtered_ldjson:
            return article_candidate
        ldjson = filtered_ldjson[0]

        article_candidate.title = ldjson.get("headling")
        article_candidate.description = ldjson.get("description")
        article_candidate.text = None
        image = ldjson.get("image")
        if isinstance(image, list) and image:
            article_candidate.topimage = image[0]['url']
        elif isinstance(image, dict):
            article_candidate.topimage = image['url']
        elif isinstance(image, str):
            article_candidate.topimage = image

        author_s = ldjson.get("author")
        if isinstance(author_s, list):
            author = [author['name'] for author in author_s if 'name' in author]
        elif type(author_s) == dict and "name" in author_s:
            author = [author_s['name']]
        else:
            author = None
        article_candidate.author = author

        publish_date = datetime.fromisoformat(ldjson.get("datePublished").replace('Z', '+00:00')) \
            if "datePublished" in ldjson \
            else None
        article_candidate.publish_date = publish_date

        article_candidate.language = ldjson.get("@language")

        return article_candidate

    @staticmethod
    def _map_ldjson(ldjson_tag: Tag):
        try:
            return json.loads(ldjson_tag.encode_contents())
        except json.JSONDecodeError:
            return None
