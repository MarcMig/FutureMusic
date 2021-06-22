import pandas as pd
from time import sleep
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium.webdriver.support.expected_conditions import element_to_be_clickable
from concurrent.futures import ThreadPoolExecutor, wait

class FutureScraper():

    def __init__(self, n, csv_name, headless=True, threads=1):
        self.n = n
        self.song_urls_df = pd.read_csv(csv_name,
        usecols=["track_name", "track_id", "track_url", "artist_name", "artist_id"]
        )
        self.dl_urls_df = pd.DataFrame()
        self.url = "https://loader.to/en3/soundcloud-downloader.html"
        self.headless = headless
        self.threads = threads

    def connect_to_base(self, browser, url):
        connection_attemps = 0

        while connection_attemps < 3:
            try:
                browser.get(url)
                WebDriverWait(browser, 5).until(presence_of_element_located((By.XPATH, '//*[@id="format"]/optgroup[1]/option[8]')))
                
                return True
            except Exception as e:
                print(e)
                connection_attemps += 10
                print(f'Error connecting to {url}')
                print(f'Attempt # {connection_attemps}')
        return False

    def write_to_file(self, dl_url, i):

            track_name = self.song_urls_df["track_name"][i]
            track_id = self.song_urls_df["track_id"][i]
            artist_name = self.song_urls_df["artist_name"][i]
            artist_id = self.song_urls_df["artist_id"][i]

            extra_data = {"track_name": track_name, "track_id": track_id,
                "download_url": dl_url, "artist_name": artist_name, "artist_id": artist_id
                }
            
            self.dl_urls_df = self.dl_urls_df.append(extra_data, ignore_index=True)
            self.dl_urls_df.to_csv(f"track_urls_{self.n}.csv")

    def run_process(self, i):

        browser = self.get_driver()

        if self.connect_to_base(browser, self.url):
            sleep(1)
            browser.find_element_by_xpath('//*[@id="link"]').send_keys(self.song_urls_df["track_url"][i])
            sleep(1)
            browser.find_element_by_xpath('//*[@id="load"]').click()

            try:
                wait = WebDriverWait(browser, 200)
                wait.until(
                    presence_of_element_located(
                        (By.XPATH, '//div[contains(@class, "download-card")]')
                    )
                )
                element_with_string = browser.find_element_by_xpath(
                    '//div[contains(@class, "download-card")]'
                )

                # Get element Id string from Download box id
                string = element_with_string.get_attribute("id").split("-")[1]
                downloadbutton_id = f"{string}_downloadLink"
                downloadbutton_xpath = f"//*[@id='{downloadbutton_id}']"

                # Get Download URL using xpath generated previously
                dl_button = f"{string}_downloadButton"
                dl_button_xpath = f"//*[@id='{dl_button}']"

                wait.until(element_to_be_clickable((By.XPATH, dl_button_xpath)))
                dl_url = browser.find_element_by_xpath(downloadbutton_xpath).get_attribute(
                    "href"
                )
                self.write_to_file(dl_url, i)
                browser.quit()
            except TimeoutError:
                print("Wait period lapsed")
                browser.quit()
             
    def get_driver(self):

        options = webdriver.ChromeOptions()
        options.add_argument("--ignore-certificate-error")
        options.add_argument("--ignore-ssl-errors")
        
        if self.headless:
            options.add_argument("--headless")
        
        return webdriver.Chrome(options=options)

    def scrape_em(self):
    
        if self.n != 'all':
            self.song_urls_df = self.song_urls_df.iloc[:self.n,]

        self.futures = []

        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            for i in range(len(self.song_urls_df)):
                self.futures.append(
                    executor.submit(self.run_process, i)
                )
        wait(self.futures)


if __name__=="__main__":
    scraper = FutureScraper(10,"only_tracks.csv", True, threads=3)
    scraper.scrape_em()
        
