import datetime
import logging

import pymysql
from scrapy.exceptions import DropItem

from newsplease.config import CrawlerConfig


class RSSCrawlCompare(object):
    """
    Compares the item's age to the current version in the DB.
    If the difference is greater than delta_time, then save the newer version.
    """
    log = None
    cfg = None
    delta_time = None
    database = None
    conn = None
    cursor = None

    # Defined DB query to retrieve the last version of the article
    compare_versions = "SELECT * FROM CurrentVersions WHERE url=%s"

    def __init__(self):
        self.log = logging.getLogger(__name__)

        self.cfg = CrawlerConfig.get_instance()
        self.delta_time = self.cfg.section("Crawler")[
            "hours_to_pass_for_redownload_by_rss_crawler"]
        self.database = self.cfg.section("MySQL")

        # Establish DB connection
        # Closing of the connection is handled once the spider closes
        self.conn = pymysql.connect(host=self.database["host"],
                                    port=self.database["port"],
                                    db=self.database["db"],
                                    user=self.database["username"],
                                    passwd=self.database["password"])
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name in ['RssCrawler', 'GdeltCrawler']:
            # Search the CurrentVersion table for a version of the article
            try:
                self.cursor.execute(self.compare_versions, (item['url'],))
            except (pymysql.err.OperationalError, pymysql.ProgrammingError, pymysql.InternalError,
                    pymysql.IntegrityError, TypeError) as error:
                self.log.error("Something went wrong in rss query: %s", error)

            # Save the result of the query. Must be done before the add,
            #   otherwise the result will be overwritten in the buffer
            old_version = self.cursor.fetchone()

            if old_version is not None and (datetime.datetime.strptime(
                    item['download_date'], "%y-%m-%d %H:%M:%S") -
                                            old_version[3]) \
                    < datetime.timedelta(hours=self.delta_time):
                # Compare the two download dates. index 3 of old_version
                # corresponds to the download_date attribute in the DB
                raise DropItem("Article in DB too recent. Not saving.")

        return item

    def close_spider(self, spider):
        # Close DB connection - garbage collection
        self.conn.close()