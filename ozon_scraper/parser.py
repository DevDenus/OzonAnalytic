import datetime

from parsel import Selector

from index_db.operations import BrandRepository, SellerRepository, ProductRepository
from .driver import ChromeDriver

with open('keywords.txt', 'r') as f:
    KEYWORDS = f.read().split('\n')


def parse_product_card(product_selector : Selector, db):
    product_info = product_selector.xpath('div')[0]
    product_url = product_info.css('a').attrib['href']
    product_primary_key = int(product_url.split('/')[-2].split('-')[-1])
    product_name = product_info.xpath('a/div').css('span::text').get()
    product_price = product_info.xpath('div')[0].xpath('div').css('span::text')[0].get().replace('\u2009', '')[:-1]
    product_brand_name = product_info.xpath('div')[1].css('b::text').get()
    continue_parsing = False
    for word in KEYWORDS:
        if word in product_name.lower() or word in product_brand_name.lower():
            continue_parsing = True
            break
    if not continue_parsing:
        return None
    if product_brand_name:
        brand_id = BrandRepository.get_or_create(db, product_brand_name).id
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
    if not (product_stored and ProductRepository.get_last_state(db, product_stored.id).hash == product_description['hash']):
        return product_url
    return None

def parse_seller(url : str, db, driver : ChromeDriver, refresh_after_seconds : int = 60*60):
    current_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    seller_stored = SellerRepository.get_by_url(db, url)
    if seller_stored is not None and (current_time - seller_stored.last_update).seconds < refresh_after_seconds:
        return []
    html_src = driver.get_page(url, 60, 3)
    response = Selector(html_src)
    seller_name = response.xpath('//div[@data-widget="sellerTransparency"]/div')[0].css('span::text').get()
    seller_id = SellerRepository.get_or_create(db, seller_name, url).id
    seller_products = response.xpath('//div[@id="contentScrollPaginator"]').css('div.tile-root')
    products_to_parse = []
    for product in seller_products:
        try:
            product_url = parse_product_card(product, db)
            if product_url is not None:
                products_to_parse.append(product_url)
        except Exception:
            continue
    return products_to_parse

def parse_brand(url : str, db, driver : ChromeDriver, refresh_after_seconds : int = 60*60):
    current_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    brand_stored = BrandRepository.get_by_url(db, url)
    if brand_stored is not None and (current_time - brand_stored.last_update).seconds < refresh_after_seconds:
        return []
    html_src = driver.get_page(url, 60, 3)
    response = Selector(html_src)
    brand_name = response.xpath('//div[@data-widget="sellerTransparency"]')[0].css('span::text').get().replace('\n', '').strip()
    brand_id = BrandRepository.get_or_create(db, brand_name, url).id
    brand_products = response.xpath('//div[@id="contentScrollPaginator"]')[0].css('div.tile-root')
    products_to_parse = []
    for product in brand_products:
        try:
            product_url = parse_product_card(product, db)
            if product_url is not None:
                products_to_parse.append(product_url)
        except Exception:
            continue
    return products_to_parse

def parse_category(url : str, db, driver : ChromeDriver):
    html_src = driver.get_page(url, 30, 3)
    response = Selector(html_src)
    category_products = response.xpath('//div[@id="contentScrollPaginator"]').css('div.tile-root')
    products_to_parse = []
    for product in category_products:
        try:
            product_url = parse_product_card(product, db)
            if product_url is not None:
                products_to_parse.append(product_url)
        except Exception:
            continue
    return products_to_parse

def parse_product(url : str, db, driver : ChromeDriver, refresh_after_seconds : int = 60*60):
    try:
        html_src = driver.get_page(url, 15)
        response = Selector(html_src)
        product_card, product_sellers = response.css('div.container.c')
    except ValueError:
        for try_cooldown in range(1, 6):
            html_src = driver.get_page(url, 15*try_cooldown, 2*try_cooldown)
            response = Selector(html_src)
            if len(response.css('div.container.c')) == 2:
                break
        else:
            print(f"Page {url} was not loaded", flush=True)
            return []
        product_card, product_sellers = response.css('div.container.c')
    current_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    links_to_parse = []
    # Scraping product
    unique_number = product_card.xpath('.//button[@data-widget="webDetailSKU"]').css('div::text').get().split()[1]
    product_on_sale = not len(product_card.xpath('//div[@data-widget="bigPromoPDP"]')) == 0
    product_block, price_block = product_card.xpath('div[@data-widget="webPdpGrid"]/div')
    product_name = product_block.xpath('.//div[@data-widget="webProductHeading"]').css('h1::text').get()
    product_rating_review = product_block.xpath('.//div[@data-widget="webSingleProductScore"]/a')
    try:
        product_rating_review = product_rating_review.css('div::text').get().split(' • ')
        product_rating = float(product_rating_review[0])
        product_reviews = int("".join(product_rating_review[1].split()[:-1]))
    except Exception:
        product_rating = 0.0
        product_reviews = 0
    product_question = product_block.xpath('.//div[@data-widget="webQuestionCount"]/a').css('div::text').get()
    if "Задать" in product_question:
        product_question_count = 0
    else:
        product_question_count = int("".join([symbol for symbol in product_question if symbol.isnumeric()]))
    product_brand = product_block.xpath('.//div[@data-widget="webBrand"]/div/div')
    if product_brand.css('a'):
        product_brand_name = product_brand.css('a::text').get()
        product_brand_url = product_brand.css('a').attrib['href']
        product_brand = BrandRepository.get_or_create(db, product_brand_name, product_brand_url)
        product_brand_id = product_brand.id
        if (current_time - product_brand.last_update).seconds >= refresh_after_seconds:
            links_to_parse.append(product_brand_url)
    else:
        product_brand_id = None
    try:
        product_price_ozon_card, product_price_other_card = price_block.xpath('.//div[@data-widget="webPrice"]/div')[0].xpath('div')
        product_price_ozon_card = product_price_ozon_card.css('span::text')[0].get().replace('\u2009', '')[:-1]
        product_price_other_card = product_price_other_card.css('span::text')[0].get().replace('\u2009', '')[:-1]
    except ValueError:
        product_price_ozon_card = price_block.xpath('.//div[@data-widget="webPrice"]/div')[0].xpath('div')
        product_price_ozon_card = product_price_other_card = product_price_ozon_card.css('span::text')[0].get().replace('\u2009', '')[:-1]
    # Scraping product's sellers
    product_seller = product_sellers.xpath('.//div[@data-widget="webCurrentSeller"]/div/div')[0].xpath('div')
    try:
        product_seller_url = product_seller.css('a').attrib['href']
    except Exception:
        product_seller_url = None
    product_seller_name = product_seller.css('a::text').get()
    seller = SellerRepository.get_or_create(db, product_seller_name, product_seller_url)
    seller_id = seller.id
    if product_seller_url and (current_time - seller.last_update).seconds >= refresh_after_seconds:
        links_to_parse.append(product_seller_url)
    other_sellers = product_sellers.xpath('.//div[@id="seller-list"]')
    more_sellers_button = None # other_sellers.xpath('button')
    if more_sellers_button is not None:
        html_src = driver.click_button_get_page('//div[@id="seller-list"]/button')
        response = Selector(html_src)
        other_sellers = response.xpath('//div[@id="seller-list"]')
    other_sellers = other_sellers.xpath('div/div')
    for seller in other_sellers:
        try:
            seller_url = seller.xpath('div/div').css('a')[0].attrib['href']
            stored_seller = SellerRepository.get_by_url(db, seller_url)
            if not stored_seller or (current_time - stored_seller.last_update).seconds >= refresh_after_seconds:
                links_to_parse.append(seller_url)
        except Exception:
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
        'brand_id' : product_brand_id
    }
    product_stored = ProductRepository.get_or_create(db, product_description)
    ProductRepository.add_state(db, product_stored.id, product_description)
    return links_to_parse

def identify_and_parse(url : str, driver : ChromeDriver, db):
    if url.startswith('/'):
        url = "https://www.ozon.ru" + url
    url_parts = url.split('/')
    if len(url_parts) < 4:
        return []
    site, target_type = url_parts[2:4]
    if site != "www.ozon.ru":
        return []

    try:
        if target_type == "product":
            new_urls = parse_product(url, db, driver)
        elif target_type == "brand":
            new_urls = parse_brand(url, db, driver)
        elif target_type == "category" or target_type == "search":
            new_urls = parse_category(url, db, driver)
        elif target_type == "seller":
            new_urls = parse_seller(url, db, driver)
        else:
            new_urls = []
        print(f"Found {len(new_urls)} urls", flush=True)
        return new_urls
    except Exception as e:
        print(f"Exception occurred in {url}: {e}", flush=True)
        return []
