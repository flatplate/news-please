from scrapy.exceptions import DropItem


class HTMLCodeHandling(object):
    """
    Handles reponses to HTML responses other than 200 (accept).
    As of 22.06.16 not active, but serves as an example of new
    functionality
    """

    def process_item(self, item, spider):
        # For the case where something goes wrong
        if item['spider_response'].status != 200:
            # Item is no longer processed in the pipeline
            raise DropItem("%s: Non-200 response" % item['url'])
        else:
            return item