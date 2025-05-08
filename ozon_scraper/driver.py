import time
import logging

from playwright.sync_api import sync_playwright

class ChromeDriver:
    def __init__(self):
        logging.getLogger("playwright").setLevel(logging.WARNING)

        self.playwright = sync_playwright().start()

        self.browser = self.playwright.chromium.launch(
            headless=False,
            args=[
                "--disable-session-crashed-bubble",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--window-size=1280,720",
                "--start-maximized",
            ]
        )
        self.context = self.browser.new_context(
            viewport={"width": 1280, "height": 720}
        )
        self.page = self.context.new_page()

    def get_page_source(self, cool_down: float = 0.2):
        time.sleep(cool_down)
        return self.page.content()

    def scrolldown_get_page(self, deep: int):
        for _ in range(deep):
            self.page.mouse.wheel(0, 250)
            time.sleep(0.1)
        self.page.wait_for_load_state('domcontentloaded')
        return self.page.content()

    def get_page(self, url: str, scroll_deep: int = 10, wait_seconds: float = 1):
        self.page.goto(url, timeout=60000)
        time.sleep(wait_seconds)
        return self.scrolldown_get_page(scroll_deep)

    def click_button_get_page(self, xpath: str, scroll_deep: int = 5):
        self.page.wait_for_selector(f'xpath={xpath}', timeout=15000)
        button = self.page.locator(f'xpath={xpath}')
        button.scroll_into_view_if_needed()
        button.click()
        return self.scrolldown_get_page(scroll_deep)

    def quit(self):
        if self.page:
            self.page.close()
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
