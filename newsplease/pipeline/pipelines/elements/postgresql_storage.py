import logging

import psycopg2

from newsplease.config import CrawlerConfig
from newsplease.pipeline.pipelines.elements.extracted_information_storage import ExtractedInformationStorage


class PostgresqlStorage(ExtractedInformationStorage):
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
    insert_current = ("INSERT INTO CurrentVersions(date_modify,date_download, \
                        localpath,filename,source_domain, \
                        url,image_url,title,title_page, \
                        title_rss,maintext,description, \
                        date_publish,authors,language, \
                        ancestor,descendant,version) \
                        VALUES (%(date_modify)s,%(date_download)s, \
                            %(localpath)s,%(filename)s,%(source_domain)s, \
                            %(url)s,%(image_url)s,%(title)s,%(title_page)s, \
                            %(title_rss)s,%(maintext)s,%(description)s, \
                            %(date_publish)s,%(authors)s,%(language)s, \
                            %(ancestor)s,%(descendant)s,%(version)s) \
                        RETURNING id")

    insert_archive = ("INSERT INTO ArchiveVersions(id,date_modify,date_download,\
                        localpath,filename,source_domain, \
                        url,image_url,title,title_page, \
                        title_rss,maintext,description, \
                        date_publish,authors,language, \
                        ancestor,descendant,version) \
                        VALUES (%(db_id)s,%(date_modify)s,%(date_download)s, \
                            %(localpath)s,%(filename)s,%(source_domain)s, \
                            %(url)s,%(image_url)s,%(title)s,%(title_page)s, \
                            %(title_rss)s,%(maintext)s,%(description)s, \
                            %(date_publish)s,%(authors)s,%(language)s, \
                            %(ancestor)s,%(descendant)s,%(version)s)")


    delete_from_current = ("DELETE FROM CurrentVersions WHERE id = %s")

    # init database connection
    def __init__(self):
        # import logging
        self.log = logging.getLogger(__name__)
        self.cfg = CrawlerConfig.get_instance()
        self.database = self.cfg.section("Postgresql")
        # Establish DB connection
        # Closing of the connection is handled once the spider closes
        self.conn = psycopg2.connect(host=self.database["host"],
                            port=self.database["port"],
                            database=self.database["database"],
                            user=self.database["user"],
                            password=self.database["password"])
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
        except psycopg2.DatabaseError as error:
            self.log.error("Something went wrong in query: %s", error)

        # Save the result of the query. Must be done before the add,
        # otherwise the result will be overwritten in the buffer
        old_version = self.cursor.fetchone()

        if old_version is not None:
            old_version_list = {
                'db_id': old_version[0],
                'date_modify': old_version[1],
                'date_download': old_version[2],
                'localpath': old_version[3],
                'filename': old_version[4],
                'source_domain': old_version[5],
                'url': old_version[6],
                'image_url': old_version[7],
                'title': old_version[8],
                'title_page': old_version[9],
                'title_rss': old_version[10],
                'maintext': old_version[11],
                'description': old_version[12],
                'date_publish': old_version[13],
                'authors': old_version[14],
                'language': old_version[15],
                'ancestor': old_version[16],
                'descendant': old_version[17],
                'version': old_version[18] }

            # Update the version number and the ancestor variable for later references
            version = (old_version[18] + 1)
            ancestor = old_version[0]

        # Add the new version of the article to the CurrentVersion table
        current_version_list = ExtractedInformationStorage.extract_relevant_info(item)
        if old_version is not None and not ExtractedInformationStorage.values_changed(
                old_version_list, current_version_list):
            return

        current_version_list['ancestor'] = ancestor
        current_version_list['descendant'] = 0
        current_version_list['version'] = version

        try:
            self.cursor.execute(self.insert_current, current_version_list)
            self.conn.commit()
            self.log.info("Article inserted into the database.")
        except psycopg2.DatabaseError as error:
            self.log.error("Something went wrong in commit: %s", error)
            self.conn.rollback()
            return

        # Move the old version from the CurrentVersion table to the ArchiveVersions table
        if old_version is not None:
            # Set descendant attribute
            try:
                old_version_list['descendant'] = self.cursor.fetchone()[0]
            except psycopg2.DatabaseError as error:
                self.log.error("Something went wrong in id query: %s", error)

            # Delete the old version of the article from the CurrentVersion table
            try:
                self.cursor.execute(self.delete_from_current, (old_version_list['db_id'],))
                self.conn.commit()
            except psycopg2.DatabaseError as error:
                self.log.error("Something went wrong in delete: %s", error)

            # Add the old version to the ArchiveVersion table
            try:
                self.cursor.execute(self.insert_archive, old_version_list)
                self.conn.commit()
                self.log.info("Moved old version of an article to the archive.")
            except psycopg2.DatabaseError as error:
                self.log.error("Something went wrong in archive: %s", error)

            try:
                self.conn.commit()
            except psycopg2.Error as e:
                self.conn.rollback()

        return item

    def close_spider(self, spider):
        # Close DB connection - garbage collection
        self.conn.close()