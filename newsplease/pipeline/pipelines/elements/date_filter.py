import datetime
import logging

from scrapy.exceptions import DropItem

from newsplease.config import CrawlerConfig


class DateFilter(object):
    """
    Filters articles based on their publishing date, articles with a date outside of a specified interval are dropped.
    This module should be placed after the KM4 article extractor.
    """

    log = None
    cfg = None
    strict_mode = False
    start_date = None
    end_date = None

    def __init__(self):
        self.log = logging.getLogger(__name__ + '.DateFilter')
        self.cfg = CrawlerConfig.get_instance()
        self.config = self.cfg.section("DateFilter")
        self.strict_mode = self.config['strict_mode']
        self.start_date = self.config['start_date']
        self.end_date = self.config['end_date']

        if self.start_date is None and self.end_date is None:
            self.log.error("DateFilter: No dates are defined, please check the configuration of this module.")
        else:
            # create datetime objects from given dates
            try:
                if self.start_date is not None:
                    self.start_date = datetime.datetime.strptime(str(self.start_date), '%Y-%m-%d %H:%M:%S')
                if self.end_date is not None:
                    self.end_date = datetime.datetime.strptime(str(self.end_date), '%Y-%m-%d %H:%M:%S')
            except ValueError as error:
                self.start_date = None
                self.end_date = None
                self.log.error("DateFilter: Couldn't read start or end date of the specified interval. "
                               "The Filter is now deactivated."
                               "Please check the configuration of this module and be sure follow the format "
                               "'yyyy-mm-dd hh:mm:ss' for dates or set the variables to None.")

    def process_item(self, item, spider):

        # Check if date could be extracted
        if item['article_publish_date'] is None and self.strict_mode:
            raise DropItem('DateFilter: %s: Publishing date is missing and strict mode is enabled.' % item['url'])
        elif item['article_publish_date'] is None:
            return item
        else:
            # Create datetime object
            try:
                publish_date = datetime.datetime.strptime(str(item['article_publish_date']), '%Y-%m-%d %H:%M:%S')
            except ValueError as error:
                self.log.warning("DateFilter: Extracted date has the wrong format: %s - %s" %
                                 (item['article_publishing_date'], item['url']))
                if self.strict_mode:
                    raise DropItem('DateFilter: %s: Dropped due to wrong date format: %s' %
                                   (item['url'], item['publish_date']))
                else:
                    return item
            # Check interval boundaries
            if self.start_date is not None and self.start_date > publish_date:
                raise DropItem('DateFilter: %s: Article is too old: %s' % (item['url'], publish_date))
            elif self.end_date is not None and self.end_date < publish_date:
                raise DropItem('DateFilter: %s: Article is too young: %s ' % (item['url'], publish_date))
            else:
                return item