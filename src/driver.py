import time
import random
from typing import Tuple

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth

class ChromeDriver:
    def __init__(self, driver_location : str, delay_bounders_sec : Tuple[float, float] = (1, 2)):
        self.options = Options()
        self.__init_options()
        self.service = Service(driver_location)
        self.driver = Chrome(service=self.service, options=self.options)
        stealth(
            self.driver,
            languages=["en-US", "en", "ru-RU"],
            vendor="Google Inc.",
            platform="Linux x86_64",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True
        )
        self.delay_bounders_sec = delay_bounders_sec

    def __init_options(self):
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")

    def random_scrolldown(self, deep : int):
        for _ in range(deep):
            scroll_dist = random.uniform(450, 550)
            self.driver.execute_script(f'window.scrollBy(0, {scroll_dist})')
            time.sleep(random.uniform(0.1, 0.3))

    def get(self, url: str, scroll_deep : int = 10):
        self.driver.get(url)
        time.sleep(random.uniform(*self.delay_bounders_sec))
        self.random_scrolldown(scroll_deep)
        html = self.driver.page_source
        return html

    def quit(self):
        self.driver.quit()

if __name__ == "__main__":
    url = "https://www.ozon.ru/product/poroshok-stiralnyy-avtomat-tide-color-80-stirok-12-kg-146781710/"
    driver_path = ChromeDriverManager().install()
    driver = ChromeDriver(driver_path)
    print(driver.get(url))
