import os.path

from newsplease.pipeline.pipelines.elements.extracted_information_storage import ExtractedInformationStorage


class HtmlFileStorage(ExtractedInformationStorage):
    """
    Handles storage of the file on the local system
    """

    # Save the html and filename to the local storage folder
    def process_item(self, item, spider):
        # Add a log entry confirming the save
        self.log.info("Saving HTML to %s", item['abs_local_path'])

        # Ensure path exists
        dir_ = os.path.dirname(item['abs_local_path'])
        os.makedirs(dir_, exist_ok=True)

        # Write raw html to local file system
        with open(item['abs_local_path'], 'wb') as file_:
            file_.write(item['spider_response'].body)

        return item