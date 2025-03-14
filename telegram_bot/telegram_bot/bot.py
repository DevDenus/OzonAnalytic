import os

import telebot
from telebot import types
from telebot.storage import StateMemoryStorage

from index_db.db import get_db
from telegram_bot.utils import (
    get_sellers_names, get_brands_names, get_product_count,
    make_seller_report, make_brand_report, make_product_report
)

DATABASE_URL = os.getenv('DATABASE_URL')
TELEGRAM_API_KEY = os.getenv('TELEGRAM_API_KEY')

state_storage = StateMemoryStorage()
bot = telebot.TeleBot(TELEGRAM_API_KEY, state_storage=state_storage)

user_states = {}

@bot.message_handler(commands=['start'])
def start_dialog(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = [
        types.KeyboardButton('/sellers_list'),
        types.KeyboardButton('/brands_list'),
        types.KeyboardButton('/seller_report'),
        types.KeyboardButton('/brand_report'),
        types.KeyboardButton('/product_report'),
        types.KeyboardButton('/product_count')
    ]
    for button in buttons:
        markup.add(button)
    bot.send_message(message.from_user.id, "Выберите действие: ", reply_markup=markup)

@bot.message_handler(commands=['sellers_list'])
def get_sellers(message):
    db = next(get_db(DATABASE_URL))
    try:
        sellers_names = get_sellers_names(db)
        reply = "\n".join(sellers_names)
        bot.send_message(message.chat.id, reply)
    finally:
        db.close()

@bot.message_handler(commands=['brands_list'])
def get_brands(message):
    db = next(get_db(DATABASE_URL))
    try:
        brands_names = get_brands_names(db)
        reply = "\n".join(brands_names)
        bot.send_message(message.chat.id, reply)
    finally:
        db.close()

@bot.message_handler(commands=['seller_report'])
def ask_for_seller(message):
    bot.send_message(message.chat.id, "Введите имя продавца, учитывая регистр")
    user_states[message.from_user.id] = "waiting_for_seller_name"
    bot.register_next_step_handler(message, process_seller)

def process_seller(message):
    seller_name = message.text
    bot.send_message(message.chat.id, f"Составляю отчёт по продавцу: {seller_name}")

    db = next(get_db(DATABASE_URL))
    try:
        report_file, file_name = make_seller_report(seller_name, db)
        bot.send_document(message.chat.id, report_file, visible_file_name=file_name)
    except KeyError:
        bot.send_message(message.chat.id, f"Продавец {seller_name} не найден")
    finally:
        db.close()
        del user_states[message.from_user.id]

@bot.message_handler(commands=['brand_report'])
def ask_for_brand(message):
    bot.send_message(message.chat.id, "Введите название брэнда, учитывая регистр")
    user_states[message.from_user.id] = "waiting_for_brand_name"
    bot.register_next_step_handler(message, process_brand)

def process_brand(message):
    brand_name = message.text
    bot.send_message(message.chat.id, f"Составляю отчёт по бренду: {brand_name}")

    db = next(get_db(DATABASE_URL))
    try:
        report_file, file_name = make_brand_report(brand_name, db)
        bot.send_document(message.chat.id, report_file, visible_file_name=file_name)
    except KeyError:
        bot.send_message(message.chat.id, f"Бренд {brand_name} не найден")
    finally:
        db.close()
        del user_states[message.from_user.id]

@bot.message_handler(commands=['product_report'])
def ask_for_product(message):
    bot.send_message(message.chat.id, "Введите артикул интересующего вас товара")
    user_states[message.from_user.id] = "waiting_product_pk"
    bot.register_next_step_handler(message, process_product)

def process_product(message):
    try:
        product_pk = int(message.text)
    except ValueError:
        bot.send_message(message.chat.id, "Артикул товара должен быть целым числом!")
        return
    bot.send_message(message.chat.id, f"Составляю отчёт по товару: {product_pk}")

    db = next(get_db(DATABASE_URL))
    try:
        report_file, file_name = make_product_report(product_pk, db)
        bot.send_document(message.chat.id, report_file, visible_file_name=file_name)
    except KeyError:
        bot.send_message(message.chat.id, f"Товар {product_pk} не найден")
    finally:
        db.close()
        del user_states[message.from_user.id]

@bot.message_handler(commands=['product_count'])
def get_products_count(message):
    db = next(get_db(DATABASE_URL))
    try:
        product_count = get_product_count(db)
        bot.send_message(message.chat.id, f"Было найдено {product_count} товаров")
    finally:
        db.close()

if __name__ == "__main__":
    bot.enable_save_next_step_handlers(delay=2)
    bot.load_next_step_handlers()
    bot.polling(none_stop=True, interval=0)
