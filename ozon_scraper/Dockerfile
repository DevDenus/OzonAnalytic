FROM ozon_index_db

WORKDIR /ozon_scraper

RUN apt update && apt install -y wget gnupg curl unzip \
    && apt install -y chromium chromium-driver xvfb \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /ozon_scraper/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

CMD ["scrapy", "crawl", "ozon"]
