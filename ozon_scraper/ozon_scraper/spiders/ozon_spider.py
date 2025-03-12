import os
import subprocess
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

import scrapy
from scrapy import Selector

from index_db.db import get_db
from index_db.operations import BrandRepository, SellerRepository, ProductRepository
from ozon_scraper.utils.driver import ChromeDriver

class OzonSpider(scrapy.Spider):
    name = "ozon"
    MAX_THREADS = int(os.getenv('MAX_THREADS'))
    DRIVER_PATH = os.getenv('DRIVER_PATH')
    DATABASE_URL = os.getenv('DATABASE_URL')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.display = subprocess.Popen(["Xvfb", ":99", "-screen", "0", "1280x720x24"])
        os.environ["DISPLAY"] = ":99"
        self.executor = ThreadPoolExecutor(self.MAX_THREADS)
        self.futures_map = {}
        self.task_queue = Queue()
        self.drivers = Queue()
        for _ in range(self.MAX_THREADS):
            self.drivers.put(ChromeDriver(self.DRIVER_PATH))

    def start_requests(self):
        start_urls = ['https://www.ozon.ru/category/telefony-i-smart-chasy-15501/']
        for url in start_urls:
            self.task_queue.put(url)
        yield from self.parse()

    def _parse_product_card(self, product_selector : Selector, db):
        product_info = product_selector.xpath('div')[0]
        product_url = product_info.css('a').attrib['href']
        product_primary_key = int(product_url.split('/')[-2].split('-')[-1])
        product_name = product_info.xpath('a/div').css('span::text').get()
        product_price = product_info.xpath('div')[0].xpath('div').css('span::text')[0].get().replace('\u2009', '')[:-1]
        product_brand = product_info.xpath('div')[1]
        if "Оригинал" in product_brand.get():
            product_brand = product_brand.xpath('span/span')[0].css('b::text').get()
            brand_id = BrandRepository.get_or_create(db, product_brand).id
        else:
            brand_id = None
        product_rating_reviews = product_info.xpath('div')[2].css('span::text')
        if product_rating_reviews:
            product_rating = float(product_rating_reviews[0].get())
            product_reviews = int(product_rating_reviews[1].get().replace('\u2009', '').split('\xa0')[0])
        else:
            product_rating = 0.0
            product_reviews = 0
        product_sections = product_selector.css('section').xpath('div/div').css('div::text')
        product_on_sale = False
        for section in product_sections:
            section_name = section.get()
            product_on_sale = 'Распродажа' in section_name
            if product_on_sale: break
        product_description = {
            'pk' : product_primary_key,
            'name' : product_name,
            'url' : product_url,
            'on_sale' : product_on_sale,
            'price' : None,
            'price_ozon_card' : product_price,
            'rating' : product_rating,
            'review_count' : product_reviews,
            'question_count' : None,
            'seller_id' : None,
            'brand_id' : brand_id,
        }
        product_description['hash'] = ProductRepository.compute_product_hash(product_description)
        product_stored = ProductRepository.get_by_pk(db, product_primary_key)
        if not (product_stored and ProductRepository.get_last_state(db, product_stored.id) == product_description['hash']):
            return product_url
        return None

    def parse_seller(self, url : str, db, driver : ChromeDriver):
        html = driver.get_page(url, 15)
        response = Selector(text=html, type='html')
        seller_name = response.xpath('//div[@data-widget="sellerTransparency"]/div')[0].css('span::text').get()
        seller_id = SellerRepository.get_or_create(db, seller_name, url).id
        seller_products = response.xpath('//div[@id="contentScrollPaginator"]').css('div.tile-root')
        products_to_parse = []
        for product in seller_products:
            product_url = self._parse_product_card(product, db)
            if product_url is not None:
                products_to_parse.append(product_url)

        return products_to_parse

    def parse_category(self, url : str, db, driver : ChromeDriver):
        html = driver.get_page(url, 15)
        response = Selector(text=html, type='html')
        category_products = response.xpath('//div[@id="contentScrollPaginator"]').css('div.tile-root')
        products_to_parse = []
        for product in category_products:
            product_url = self._parse_product_card(product, db)
            if product_url is not None:
                products_to_parse.append(product_url)

        return products_to_parse

    def parse_brand(self, url : str, db, driver : ChromeDriver):
        html = driver.get(url, 15)
        response = Selector(text=html, type='html')
        brand_name = response.xpath('//div[@data-widget="meta"]/div/div')[1].css('h1::text').get().replace('\n', '').strip()
        brand_id = BrandRepository.get_or_create(db, brand_name, url).id
        brand_products = response.xpath('//div[@id="contentScrollPaginator"]').css('div.tile-root')
        products_to_parse = []
        for product in brand_products:
            product_url = self._parse_product_card(product, db)
            if product_url is not None:
                products_to_parse.append(product_url)

        return products_to_parse

    def parse_product(self, url : str, db, driver : ChromeDriver):
        html = driver.get_page(url, 5)
        response = Selector(text=html, type='html')
        product_card, product_sellers = response.css('div.container.c')
        # Scraping product
        unique_number = product_card.xpath('.//button[@data-widget="webDetailSKU"]').css('div::text').get().split()[1]
        product_on_sale = not len(product_card.xpath('//div[@data-widget="bigPromoPDP"]')) == 0
        product_block, price_block = product_card.xpath('div[@data-widget="webPdpGrid"]/div')
        product_name = product_block.xpath('.//div[@data-widget="webProductHeading"]').css('h1::text').get().replace('\n', '').strip()
        product_rating_review = product_block.xpath('.//div[@data-widget="webSingleProductScore"]/a').css('div::text').get().split(' • ')
        if len(product_rating_review):
            product_rating = float(product_rating_review[0])
            product_reviews = int("".join(product_rating_review[1].split()[:-1]))
        else:
            product_rating = 0.0
            product_reviews = 0
        product_question_count = "".join(product_block.xpath('.//div[@data-widget="webQuestionCount"]/a').css('div::text').get().split()[:-1])
        if product_question_count.isdigit():
            product_question_count = int(product_question_count)
        else:
            product_question_count = 0
        product_brand = product_block.xpath('.//div[@data-widget="webBrand"]/div/div').css('a::text').get()
        product_price_ozon_card, product_price_other_card = price_block.xpath('.//div[@data-widget="webPrice"]/div')[0].xpath('div')
        product_price_ozon_card = product_price_ozon_card.css('span::text')[0].get().replace('\u2009', '')[:-1]
        product_price_other_card = product_price_other_card.css('span::text')[0].get().replace('\u2009', '')[:-1]
        # Scraping product's sellers
        product_seller = product_sellers.xpath('.//div[@data-widget="webCurrentSeller"]/div/div')[0].xpath('div')
        product_seller_url = product_seller.css('a').attrib['href']
        product_seller_name = product_seller.css('a::text').get()
        seller_id = SellerRepository.get_or_create(db, product_seller_name, product_seller_url).id
        sellers_to_parse = [product_seller_url] if product_seller_url else []
        other_sellers = product_sellers.xpath('.//div[@id="seller-list"]')
        more_sellers_button = other_sellers.xpath('button')
        if more_sellers_button:
            html = driver.click_button_get_page('//div[@id="seller-list"]/button')
            response = Selector(text=html, type='html')
            other_sellers = response.xpath('//div[@id="seller-list"]')
        other_sellers = other_sellers.xpath('div/div')
        for seller in other_sellers:
            try:
                seller_url = seller.xpath('div/div')[1].css('a').attrib['href']
                sellers_to_parse.append(seller_url)
            except KeyError:
                continue

        product_description = {
            'pk' : int(unique_number),
            'name' : product_name,
            'url' : url,
            'on_sale' : product_on_sale,
            'price' : product_price_other_card,
            'price_ozon_card' : product_price_ozon_card,
            'rating' : product_rating,
            'review_count' : product_reviews,
            'question_count' : product_question_count,
            'seller_id' : seller_id,
            'brand_id' : product_brand
        }
        product_stored = ProductRepository.get_or_create(db, product_description)
        ProductRepository.add_state(db, product_stored.id, product_description)

        return sellers_to_parse

    def identify_and_parse(self, url : str):
        if url.startswith('/'):
            url = "https://www.ozon.ru" + url
        url_parts = url.split('/')
        if len(url_parts) < 4:
            return []
        site, target_type = url_parts[2:4]
        if site != "www.ozon.ru":
            return []

        driver = self.drivers.get()
        db = next(get_db(self.DATABASE_URL))
        try:
            if target_type == "product":
                new_urls = self.parse_product(url, db, driver)
            elif target_type == "brand":
                new_urls = self.parse_brand(url, db, driver)
            elif target_type == "category":
                new_urls = self.parse_category(url, db, driver)
            elif target_type == "seller":
                new_urls = self.parse_seller(url, db, driver)
            else:
                new_urls = []
            print(f"Found {len(new_urls)} urls")
            return new_urls
        finally:
            db.close()
            self.drivers.put(driver)

    def parse(self):
        while True:
            while not self.task_queue.empty() and len(self.futures_map) < self.MAX_THREADS:
                next_url = self.task_queue.get()
                future = self.executor.submit(self.identify_and_parse, next_url)
                self.futures_map[future] = next_url

            if not self.futures_map and self.task_queue.empty():
                break

            for future in as_completed(self.futures_map):
                try:
                    new_urls = future.result()
                    if new_urls:
                        for new_url in new_urls:
                            self.task_queue.put(new_url)
                            yield {"url" : new_url}
                except Exception as e:
                    print(f"Exception occurred: {e}")
                del self.futures_map[future]

            yield None

        print("Scrapping is done!")
        self.crawler.engine.unpause()

    def closed(self, reason):
        self.executor.shutdown(wait=True)
        while not self.drivers.empty():
            driver = self.drivers.get()
            driver.quit()
        self.display.terminate()
