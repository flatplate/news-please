import logging
import sys

from elasticsearch import Elasticsearch

from newsplease.config import CrawlerConfig
from newsplease.pipeline.pipelines.elements.extracted_information_storage import ExtractedInformationStorage

if sys.version_info[0] < 3:
    ConnectionError = OSError

class ElasticsearchStorage(ExtractedInformationStorage):
    """
    Handles remote storage of the meta data in Elasticsearch
    """

    log = None
    cfg = None
    es = None
    index_current = None
    index_archive = None
    mapping = None
    running = False

    def __init__(self):
        self.log = logging.getLogger('elasticsearch.trace')
        self.log.addHandler(logging.NullHandler())
        self.cfg = CrawlerConfig.get_instance()
        self.database = self.cfg.section("Elasticsearch")

        self.es = Elasticsearch(
            [self.database["host"]],
            http_auth=(str(self.database["username"]), str(self.database["secret"])),
            port=self.database["port"],
            use_ssl=self.database["use_ca_certificates"],
            verify_certs=self.database["use_ca_certificates"],
            ca_certs=self.database["ca_cert_path"],
            client_cert=self.database["client_cert_path"],
            client_key=self.database["client_key_path"]
        )
        self.index_current = self.database["index_current"]
        self.index_archive = self.database["index_archive"]
        self.mapping = self.database["mapping"]

        # check connection to Database and set the configuration

        try:
            # check if server is available
            self.es.ping()

            # raise logging level due to indices.exists() habit of logging a warning if an index doesn't exist.
            es_log = logging.getLogger('elasticsearch')
            es_level = es_log.getEffectiveLevel()
            es_log.setLevel('ERROR')

            # check if the necessary indices exist and create them if needed
            if not self.es.indices.exists(self.index_current):
                self.es.indices.create(index=self.index_current, ignore=[400, 404])
                self.es.indices.put_mapping(index=self.index_current, body=self.mapping)
            if not self.es.indices.exists(self.index_archive):
                self.es.indices.create(index=self.index_archive, ignore=[400, 404])
                self.es.indices.put_mapping(index=self.index_archive, body=self.mapping)
            self.running = True

            # restore previous logging level
            es_log.setLevel(es_level)

        except ConnectionError as error:
            self.running = False
            self.log.error("Failed to connect to Elasticsearch, this module will be deactivated. "
                           "Please check if the database is running and the config is correct: %s" % error)

    def process_item(self, item, spider):

        if self.running:
            try:
                version = 1
                ancestor = None

                # search for previous version
                request = self.es.search(index=self.index_current, body={'query': {'match': {'url.keyword': item['url']}}})
                if request['hits']['total']['value'] > 0:
                    # save old version into index_archive
                    old_version = request['hits']['hits'][0]
                    old_version['_source']['descendent'] = True
                    self.es.index(index=self.index_archive, doc_type='_doc', body=old_version['_source'])
                    version += 1
                    ancestor = old_version['_id']

                # save new version into old id of index_current
                self.log.info("Saving to Elasticsearch: %s" % item['url'])
                extracted_info = ExtractedInformationStorage.extract_relevant_info(item)
                extracted_info['ancestor'] = ancestor
                extracted_info['version'] = version
                self.es.index(index=self.index_current, doc_type='_doc', id=ancestor,
                              body=extracted_info)


            except ConnectionError as error:
                self.running = False
                self.log.error("Lost connection to Elasticsearch, this module will be deactivated: %s" % error)
        return item