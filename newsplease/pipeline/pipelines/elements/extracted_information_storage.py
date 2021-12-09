import logging

from dateutil import parser as dateparser

from newsplease.config import CrawlerConfig


class ExtractedInformationStorage(object):
    """
    Provides basic functionality for Storages
    """

    log = None

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.log.addHandler(logging.NullHandler())
        self.cfg = CrawlerConfig.get_instance()

    @staticmethod
    def ensure_str(text):
        if isinstance(text, str):
            return text
        else:
            return text.decode('utf-8')

    @staticmethod
    def extract_relevant_info(item):
        """
        extracts from an item only fields that we want to output as extracted information
        :rtype: object
        :param item:
        :return:
        """
        article = {
            'authors': item['article_author'],
            'date_download': item['download_date'],
            'date_modify': item['modified_date'],
            'date_publish': item['article_publish_date'],
            'description': item['article_description'],
            'filename': item['filename'],
            'image_url': item['article_image'],
            'language': item['article_language'],
            'localpath': item['local_path'],
            'title': item['article_title'],
            'title_page': ExtractedInformationStorage.ensure_str(item['html_title']),
            'title_rss': ExtractedInformationStorage.ensure_str(item['rss_title']),
            'source_domain': ExtractedInformationStorage.ensure_str(item['source_domain']),
            'maintext': item['article_text'],
            'url': item['url']
        }

        # clean values
        for key in article:
            value = article[key]
            if isinstance(value, str) and not value:
                article[key] = None

        return article

    @staticmethod
    def datestring_to_date(text):
        if text:
            return dateparser.parse(text)
        else:
            return None

    @staticmethod
    def convert_to_class(item):
        news_article = NewsArticle()
        news_article.authors = item['authors']
        news_article.date_download = ExtractedInformationStorage.datestring_to_date(item['date_download'])
        news_article.date_modify = ExtractedInformationStorage.datestring_to_date(item['date_modify'])
        news_article.date_publish = ExtractedInformationStorage.datestring_to_date(item['date_publish'])
        news_article.description = item['description']
        news_article.filename = item['filename']
        news_article.image_url = item['image_url']
        news_article.language = item['language']
        news_article.localpath = item['localpath']
        news_article.title = item['title']
        news_article.title_page = item['title_page']
        news_article.title_rss = item['title_rss']
        news_article.source_domain = item['source_domain']
        news_article.maintext = item['maintext']
        news_article.url = item['url']
        return news_article