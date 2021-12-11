import datetime
import logging

import psycopg2
from scrapy.exceptions import DropItem, IgnoreRequest

from newsplease.config import CrawlerConfig


class RSSCrawlComparePostgres(object):
    """
    Compares the item's age to the current version in the DB.
    If the difference is greater than delta_time, then save the newer version.
    TODO unify this and RssCrawlCompare by introducing repositories and decoupling
    this class from the underlying database that is used
    TODO Move this to middlewares
    """
    log = None
    cfg = None
    delta_time = None
    database = None
    conn = None
    cursor = None

    # Defined DB query to retrieve the last version of the article
    compare_versions = "SELECT date_download FROM CurrentVersions WHERE url=%s"

    def __init__(self):
        self.log = logging.getLogger(__name__)

        self.cfg = CrawlerConfig.get_instance()
        self.delta_time = self.cfg.section("Crawler")["hours_to_pass_for_redownload_by_rss_crawler"]
        self.database = self.cfg.section("Postgresql")

        # Establish DB connection
        # Closing of the connection is handled once the spider closes
        self.conn = psycopg2.connect(host=self.database["host"],
                                     port=self.database["port"],
                                     dbname=self.database["database"],
                                     user=self.database["user"],
                                     password=self.database["password"])
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name in ['RssCrawler', 'GdeltCrawler']:
            # Search the CurrentVersion table for a version of the article
            try:
                self.cursor.execute(self.compare_versions, (item['url'],))
            except (psycopg2.OperationalError, TypeError) as error:
                self.log.error("Something went wrong in rss query: %s", error)

            # Save the result of the query. Must be done before the add,
            #   otherwise the result will be overwritten in the buffer
            old_version = self.cursor.fetchone()

            if old_version is not None and (datetime.datetime.strptime(
                    item['download_date'], "%y-%m-%d %H:%M:%S") -
                                            old_version[0]) \
                    < datetime.timedelta(hours=self.delta_time):
                # Compare the two download dates. index 3 of old_version
                # corresponds to the download_date attribute in the DB
                raise DropItem("Article in DB too recent. Not saving.")

        return item

    def close_spider(self, spider):
        # Close DB connection - garbage collection
        self.conn.close()

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_request(self, request, spider):
        if spider.name in ['RssCrawler', 'GdeltCrawler']:
            # Search the CurrentVersion table for a version of the article
            try:
                self.cursor.execute(self.compare_versions, (request.url,))
            except (psycopg2.OperationalError, TypeError) as error:
                self.log.error("Something went wrong in rss query: %s", error)

            # Save the result of the query. Must be done before the add,
            #   otherwise the result will be overwritten in the buffer
            old_version = self.cursor.fetchone()

            if old_version is not None \
                    and datetime.datetime.now() - old_version[0] < datetime.timedelta(hours=self.delta_time):
                # Compare the two download dates. index 3 of old_version
                # corresponds to the download_date attribute in the DB
                self.log.debug("Ignoring request, article in DB too recent")
                raise IgnoreRequest("Article in DB too recent. Not downloading.")
