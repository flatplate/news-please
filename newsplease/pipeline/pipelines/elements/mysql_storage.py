import logging

import pymysql

from newsplease.config import CrawlerConfig


class MySQLStorage(object):
    """
    Handles remote storage of the meta data in the DB
    """

    log = None
    cfg = None
    database = None
    conn = None
    cursor = None
    # initialize necessary DB queries for this pipe
    compare_versions = ("SELECT * FROM CurrentVersions WHERE url=%s")
    insert_current = ("INSERT INTO CurrentVersions(local_path,\
                          modified_date,download_date,source_domain,url,\
                          html_title, ancestor, descendant, version,\
                          rss_title) VALUES (%(local_path)s,\
                          %(modified_date)s, %(download_date)s,\
                          %(source_domain)s, %(url)s, %(html_title)s,\
                          %(ancestor)s, %(descendant)s, %(version)s,\
                          %(rss_title)s)")

    insert_archive = ("INSERT INTO ArchiveVersions(id, local_path,\
                          modified_date,download_date,source_domain,url,\
                          html_title, ancestor, descendant, version,\
                          rss_title) VALUES (%(db_id)s, %(local_path)s,\
                          %(modified_date)s, %(download_date)s,\
                          %(source_domain)s, %(url)s, %(html_title)s,\
                          %(ancestor)s, %(descendant)s, %(version)s,\
                          %(rss_title)s)")

    delete_from_current = ("DELETE FROM CurrentVersions WHERE id = %s")

    # init database connection
    def __init__(self):
        self.log = logging.getLogger(__name__)

        self.cfg = CrawlerConfig.get_instance()
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
        """
        Store item data in DB.
        First determine if a version of the article already exists,
          if so then 'migrate' the older version to the archive table.
        Second store the new article in the current version table
        """

        # Set defaults
        version = 1
        ancestor = 0

        # Search the CurrentVersion table for an old version of the article
        try:
            self.cursor.execute(self.compare_versions, (item['url'],))
        except (pymysql.err.OperationalError, pymysql.ProgrammingError, pymysql.InternalError,
                pymysql.IntegrityError, TypeError) as error:
            self.log.error("Something went wrong in query: %s", error)

        # Save the result of the query. Must be done before the add,
        # otherwise the result will be overwritten in the buffer
        old_version = self.cursor.fetchone()

        if old_version is not None:
            old_version_list = {
                'db_id': old_version[0],
                'local_path': old_version[1],
                'modified_date': old_version[2],
                'download_date': old_version[3],
                'source_domain': old_version[4],
                'url': old_version[5],
                'html_title': old_version[6],
                'ancestor': old_version[7],
                'descendant': old_version[8],
                'version': old_version[9],
                'rss_title': old_version[10], }

            # Update the version number and the ancestor variable for later references
            version = (old_version[9] + 1)
            ancestor = old_version[0]

        # Add the new version of the article to the CurrentVersion table
        current_version_list = {
            'local_path': item['local_path'],
            'modified_date': item['modified_date'],
            'download_date': item['download_date'],
            'source_domain': item['source_domain'],
            'url': item['url'],
            'html_title': item['html_title'],
            'ancestor': ancestor,
            'descendant': 0,
            'version': version,
            'rss_title': item['rss_title'], }

        try:
            self.cursor.execute(self.insert_current, current_version_list)
            self.conn.commit()
            self.log.info("Article inserted into the database.")
        except (pymysql.err.OperationalError, pymysql.ProgrammingError, pymysql.InternalError,
                pymysql.IntegrityError, TypeError) as error:
            self.log.error("Something went wrong in commit: %s", error)

        # Move the old version from the CurrentVersion table to the ArchiveVersions table
        if old_version is not None:
            # Set descendant attribute
            try:
                old_version_list['descendant'] = self.cursor.lastrowid
            except (pymysql.err.OperationalError, pymysql.ProgrammingError, pymysql.InternalError,
                    pymysql.IntegrityError, TypeError) as error:
                self.log.error("Something went wrong in id query: %s", error)

            # Delete the old version of the article from the CurrentVersion table
            try:
                self.cursor.execute(self.delete_from_current, old_version_list['db_id'])
                self.conn.commit()
            except (pymysql.err.OperationalError, pymysql.ProgrammingError, pymysql.InternalError,
                    pymysql.IntegrityError, TypeError) as error:
                self.log.error("Something went wrong in delete: %s", error)

            # Add the old version to the ArchiveVersion table
            try:
                self.cursor.execute(self.insert_archive, old_version_list)
                self.conn.commit()
                self.log.info("Moved old version of an article to the archive.")
            except (pymysql.err.OperationalError, pymysql.ProgrammingError, pymysql.InternalError,
                    pymysql.IntegrityError, TypeError) as error:
                self.log.error("Something went wrong in archive: %s", error)

        return item

    def close_spider(self, spider):
        # Close DB connection - garbage collection
        self.conn.close()