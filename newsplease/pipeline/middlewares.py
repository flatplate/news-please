from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

class TooManyRequestsRetryMiddleware(RetryMiddleware):
    def __init__(self, crawler):
        super(TooManyRequestsRetryMiddleware, self).__init__(crawler.settings)
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        elif response.status == 429:
            reason = response_status_message(response.status)
            request = self._retry(request, reason, spider)
            if request:
                request.meta['dont_proxy'] = False
                return request
            return response

        elif response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response

class DontProxyMiddleware:
    def __init__(self):
        pass

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_response(self, request, response, spider):
        request.meta['dont_proxy'] = True
        return request