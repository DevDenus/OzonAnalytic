import os
from threading import Thread
from dotenv import load_dotenv

from index_db.db import init_db
from ozon_scraper.crawler import crawl
from telegram_bot.bot import bot

load_dotenv()

START_URLS = os.getenv('START_URLS').split(',')
DATABASE_URL = os.getenv('DATABASE_URL')

def start_bot():
    bot.enable_save_next_step_handlers(delay=2)
    bot.load_next_step_handlers()
    bot.polling(none_stop=True, interval=0)

def start_crawler():
    crawl(START_URLS, DATABASE_URL)

def main():
    init_db(DATABASE_URL)

    bot_thread = Thread(target=start_bot)
    bot_thread.start()
    crawler_thread = Thread(target=start_crawler)
    crawler_thread.start()

    bot_thread.join()
    crawler_thread.join()

if __name__ == "__main__":
    main()
