from typing import Tuple, List
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

import scrapy

from ozon_scraper.utils.driver import ChromeDriver

class OzonSpider(scrapy.Spider):
    name = "ozon"
    MAX_THREADS = 2
    MAX_TASKS = 100
    driver_path = "/usr/bin/chromedriver"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.executor = ThreadPoolExecutor(self.MAX_THREADS)
        self.task_queue = Queue(self.MAX_TASKS)
        self.drivers = Queue()
        for _ in range(self.MAX_THREADS):
            self.drivers.put(ChromeDriver(self.driver_path))

    def start_requests(self):
        start_urls = ['https://www.ozon.ru/category/telefony-i-smart-chasy-15501/']
        yield from self.parse(start_urls)

    def _get_page_source(self, url : str, scroll_down : int) -> Tuple[str, str]:
        """Gets page html sources, using ChromeDrive instance

        Args:
            url (str): page url
            scroll_down (int): times to scroll

        Returns:
            url (str): page url
            html (str) : page source code
        """
        driver = self.drivers.get()
        try:
            html = driver.get(url, scroll_down)
        finally:
            self.drivers.put(driver)
        return url, html

    def _parse_product(self, url : str):
        _, html = self._get_page_source(url, 15)
        sel = scrapy.Selector(text=html, type='html')
        container = sel.css('div.container.c')
        unique_number = container.xpath('.//button[@data-widget="webDetailSKU"]').css('div::text').get().split()[1]

        paginator = sel.xpath()




    def _parse_seller(self, url : str):
        _, html = self._get_page_source(url, 15)
        sel = scrapy.Selector(text=html, type='html')

    def _parse_category(self, url : str):
        _, html = self._get_page_source(url, 30)
        sel = scrapy.Selector(text=html, type='html')
        container = sel.css('div.container.c')
        paginator = container.xpath('div[@id="paginatorContent"]')
        search_results = paginator.xpath('div[@data-widget="searchResultsV2"]')
        for search_result in search_results:
            products = search_result.xpath('div/div')
            for product in products:
                product_url = product.css('a::attr(href)').get()
                product_description = product.xpath('div')[0]
                product_price = product_description.xpath('div/div').css('span::text').get().replace('\u2009', '')
                product_name = product_description.xpath(f'.//a[href={product_url}]/div').css('span::text').get()



    def _parse_brand(self, url : str):
        _, html = self._get_page_source(url, 15)
        sel = scrapy.Selector(text=html, type='html')

    def _parse_highlight(self, url : str):
        _, html = self._get_page_source(url, 15)
        sel = scrapy.Selector(text=html, type='html')

    def _identify_and_parse(self, url : str):
        site, target_type = url.split('/')[2:4]
        if site != "www.ozon.ru":
            return
        if target_type == "product":
            return self._parse_product(url)
        elif target_type == "brand":
            return self._parse_brand(url)
        elif target_type == "category":
            return self._parse_category(url)
        elif target_type == "seller":
            return self._parse_seller(url)
        elif target_type == "highlight":
            return self._parse_highlight(url)
        return

    def parse(self, urls : List[str]):
        futures = {
            self.executor.submit(self._identify_and_parse, url) : url
            for url in urls
        }





    def closed(self, reason):
        while not self.drivers.empty():
            driver = self.drivers.get()
            driver.quit()
        self.executor.shutdown()
