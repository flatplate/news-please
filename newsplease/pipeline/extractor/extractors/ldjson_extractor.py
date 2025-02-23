from .abstract_extractor import AbstractExtractor
from ..article_candidate import ArticleCandidate
from bs4 import BeautifulSoup
from bs4.element import Tag
import json
from dateutil.parser import parse
import re


class LdjsonExtractor(AbstractExtractor):
    """
    Extractor that uses the ld+json data inside a web page
    to extract metadata.
    """
    # TODO Move the ldjson extraction to helpers since it is also used in heuristics and is duplicate

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

        soup = BeautifulSoup(item['spider_response'].body, parser="lxml", features="lxml")
        ldjson_candidates = soup.select('script[type="application/ld+json"]')

        if not ldjson_candidates:
            return article_candidate

        parsed_ldjson = [self._map_ldjson(ldjson_tag) for ldjson_tag in ldjson_candidates]
        for single_ldjson in parsed_ldjson:
            if hasattr(single_ldjson, '__contains__') and "@graph" in single_ldjson:
                if isinstance(single_ldjson["@graph"], list):
                    parsed_ldjson.extend(single_ldjson["@graph"])
                elif isinstance(single_ldjson["@graph"], dict):
                    parsed_ldjson.append(single_ldjson["@graph"])
        filtered_ldjson = [ldjson for ldjson in parsed_ldjson
                           if "@type" in ldjson and ldjson["@type"] in ["NewsArticle", "Article"]]

        if not filtered_ldjson:
            return article_candidate
        ldjson = filtered_ldjson[0]

        article_candidate.title = ldjson.get("headline")
        article_candidate.description = ldjson.get("description")
        article_candidate.text = None
        image = ldjson.get("image")
        if isinstance(image, list) and image:
            if isinstance(image[0], str):
                article_candidate.topimage = image[0]
            elif isinstance(image[0], dict):
                article_candidate.topimage = image[0]['url']
        elif isinstance(image, dict):
            article_candidate.topimage = image.get('url')
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

        publish_date = self.parse_datestring(ldjson.get("datePublished").replace('Z', '+00:00')) \
            if "datePublished" in ldjson \
            else None
        article_candidate.publish_date = publish_date

        article_candidate.language = ldjson.get("@language")

        return article_candidate

    def parse_datestring(self, datestring):
        try:
            return parse(datestring)
        except:  # TODO
            if "GMT+" in datestring:
                parts = re.split("GMT\\+\\d{4}", datestring)
                return parse(parts[0] + "T" + parts[1])
            return None

    @staticmethod
    def _map_ldjson(ldjson_tag: Tag):
        try:
            return json.loads(ldjson_tag.encode_contents())
        except json.JSONDecodeError:
            return None
