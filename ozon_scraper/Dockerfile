FROM ozon_index_db

WORKDIR /app

RUN apt-get update && apt-get install -y wget gnupg curl unzip \
    && apt-get install -y chromium chromium-driver xvfb \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

CMD ["scrapy", "crawl", "ozon"]
