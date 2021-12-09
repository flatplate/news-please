from newsplease.pipeline.pipelines.elements.extracted_information_storage import ExtractedInformationStorage


class InMemoryStorage(ExtractedInformationStorage):
    """
    Stores extracted information in a dictionary in memory - for use with library mode.
    """

    results = {}  # this is a static variable

    def process_item(self, item, spider):
        # get the original url, so that the library class (or whoever wants to read this) can access the article
        if 'redirect_urls' in item._values['spider_response'].meta:
            url = item._values['spider_response'].meta['redirect_urls'][0]
        else:
            url = item._values['url']
        InMemoryStorage.results[url] = ExtractedInformationStorage.extract_relevant_info(item)
        return item

    @staticmethod
    def get_results():
        return InMemoryStorage.results