import json
import os.path

from newsplease.pipeline.pipelines.elements.extracted_information_storage import ExtractedInformationStorage


class JsonFileStorage(ExtractedInformationStorage):
    """
    Handles remote storage of the data in Json files
    """

    log = None
    cfg = None

    def process_item(self, item, spider):
        file_path = item['abs_local_path'] + '.json'

        # Add a log entry confirming the save
        self.log.info("Saving JSON to %s", file_path)

        # Ensure path exists
        dir_ = os.path.dirname(item['abs_local_path'])
        os.makedirs(dir_, exist_ok=True)

        # Write JSON to local file system
        with open(file_path, 'w') as file_:
            json.dump(ExtractedInformationStorage.extract_relevant_info(item), file_, ensure_ascii=False)

        return item