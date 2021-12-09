import logging
import os.path

from newsplease.config import CrawlerConfig
from newsplease.pipeline.pipelines.elements.extracted_information_storage import ExtractedInformationStorage

try:
    import numpy as np
    import pandas as pd
except ImportError:
    np = None
    pd = None


class PandasStorage(ExtractedInformationStorage):
    """
    Store meta data a Pandas data frame
    """

    log = None
    cfg = None
    es = None
    index_current = None
    index_archive = None
    mapping = None
    running = False

    def __init__(self):
        if np is None:
            raise ModuleNotFoundError("Using PandasStorage requires numpy and pandas")
        self.log = logging.getLogger(__name__)
        self.cfg = CrawlerConfig.get_instance()
        self.database = self.cfg.section("Pandas")

        df_index = "url"
        columns = [
            "source_domain", "title_page", "title_rss", "localpath", "filename",
            "date_download", "date_modify", "date_publish", "title", "description",
            "text", "authors", "image_url", "language", 'url'
        ]

        working_path = self.cfg.section("Files")['working_path']
        file_name = self.database['file_name']
        self.full_path = os.path.join(working_path, file_name, '.pickle')

        try:
            self.df = pd.read_pickle(self.full_path)
            self.log.info(
                "Found existing Pandas file with %i rows at %s", len(self.df),
                self.full_path
            )
            for col in columns:
                if col not in self.df.columns:
                    raise KeyError(col)
        except FileNotFoundError:
            self.df = pd.DataFrame(columns=columns.keys())
            self.log.info("Created new Pandas file at '%s'", self.full_path)
            self.df.set_index(df_index, inplace=True, drop=False)
        except KeyError as e:
            self.log.error("%s is missing a column.", self.full_path)
            raise e

    def process_item(self, item, _spider):
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
            'source_domain':
            ExtractedInformationStorage.ensure_str(item['source_domain']),
            'text': item['article_text'],
            'url': item['url']
        }
        self.df.loc[item['url']] = article
        return item

    def close_spider(self, _spider):
        """
        Write out to file
        """
        self.df['date_download'] = pd.to_datetime(
            self.df['date_download'], errors='coerce', infer_datetime_format=True
        )
        self.df['date_modify'] = pd.to_datetime(
            self.df['date_modify'], errors='coerce', infer_datetime_format=True
        )
        self.df['date_publish'] = pd.to_datetime(
            self.df['date_publish'], errors='coerce', infer_datetime_format=True
        )
        self.df.to_pickle(self.full_path)
        self.log.info("Wrote to Pandas to %s", self.full_path)