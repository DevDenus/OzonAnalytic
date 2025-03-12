import time
import logging
import random
import os
import subprocess
import shutil
from typing import Tuple

from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
from fake_useragent import UserAgent

class ChromeDriver:
    def __init__(self, driver_location : str, delay_bounders_sec : Tuple[float, float] = (0, 0.5)):
        logging.getLogger("selenium").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

        self.user_agent = UserAgent(browsers=['chrome']).random
        self.options = Options()
        self.__init_options()
        self.service = Service(driver_location)
        self.driver = Chrome(service=self.service, options=self.options)
        stealth(
            self.driver,
            user_agent=self.user_agent,
            languages=["en-US", "en", "ru-RU"],
            vendor="Google Inc.",
            platform="Linux x86_64",
            webgl_vendor="Google LLC",
            renderer="ANGLE (Intel, Intel(R) UHD Graphics, OpenGL 4.5)",
            fix_hairline=True
        )

        self.delay_bounders_sec = delay_bounders_sec

    def __init_options(self):
        base_temp_dir = "/tmp"
        unique_dir = os.path.join(base_temp_dir, f"chrome_profile_{random.randint(10000, 99999)}")

        if os.path.exists(unique_dir):
            shutil.rmtree(unique_dir)

        os.makedirs(unique_dir, exist_ok=True, mode=0o777)

        self.options.add_argument(f"--user-data-dir={unique_dir}")
        self.options.add_argument(f"--user-agent={self.user_agent}")
        self.options.add_argument("--disable-gpu")
        self.options.add_argument("--disable-session-crashed-bubble")
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--window-size=1280,720")
        self.options.add_argument("--start-maximized")
        self.options.add_argument("--log-level=3")

    def scrolldown_get_page(self, deep : int):
        for _ in range(deep):
            self.driver.execute_script(f'window.scrollBy(0, 500)')
            time.sleep(0.1)
        html = self.driver.page_source
        return html

    def get_page(self, url: str, scroll_deep : int = 10):
        self.driver.get(url)
        time.sleep(random.uniform(*self.delay_bounders_sec))
        html = self.scrolldown_get_page(scroll_deep)
        return html

    def click_button_get_page(self, xpath : str, scroll_deep : int = 5):
        button = self.driver.find_element(By.XPATH, xpath)
        self.driver.execute_script("arguments[0].scrollIntoView();", button)
        button.click()
        html = self.scrolldown_get_page(scroll_deep)
        return html

    def quit(self):
        if self.driver:
            self.driver.quit()
