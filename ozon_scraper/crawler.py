from queue import Queue
from typing import List

from index_db.db import get_db
from ozon_scraper.driver import ChromeDriver
from ozon_scraper.parser import identify_and_parse


def crawl(start_urls : List[str], database_url : str):
    task_queue = Queue()
    visited_urls = set()
    driver = ChromeDriver()
    db = next(get_db(database_url))
    for url in start_urls:
        task_queue.put(url)

    try:
        while not task_queue.empty():
            url = task_queue.get()
            new_urls = identify_and_parse(url, driver, db)
            visited_urls.add(url)
            for url in new_urls:
                if not url in visited_urls:
                    task_queue.put(url)
    finally:
        db.close()
        driver.quit()
