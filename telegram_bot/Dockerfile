FROM ozon_index_db

WORKDIR /telegram_bot

COPY requirements.txt /telegram_bot/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

ENV PYTHONPATH=/telegram_bot

CMD ["python", "telegram_bot/bot.py"]
