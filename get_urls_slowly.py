import pandas as pd
from time import sleep
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium.webdriver.support.expected_conditions import element_to_be_clickable


def scrape_song_urls(n="all", verbose=True, csv_name):

    # Get list song Urls from DataFrame
    song_urls = pd.read_csv(csv_name, usecols=["track_name", "track_id", "track_url", "artist_name", "artist_id"])
    song_urls_df = pd.DataFrame()
    options = webdriver.ChromeOptions()
    options.add_argument("--ignore-certificate-error")
    options.add_argument("--ignore-ssl-errors")
    driver = webdriver.Chrome(options=options)

    if n != "all":
        song_urls = song_urls.iloc[
            :n,
        ]

    url = "https://loader.to/en3/soundcloud-downloader.html"

    for i in range(len(song_urls)):

        # Go to webpage, find elements
        track_name = song_urls["track_name"][i]
        track_id = song_urls["track_id"][i]
        track_url = song_urls["track_url"][i]
        artist_name = song_urls["artist_name"][i]
        artist_id = song_urls["artist_id"][i]

        driver.get(url)
        sleep(1)
        driver.find_element_by_xpath('//*[@id="format"]/optgroup[1]/option[8]').click()
        driver.find_element_by_xpath('//*[@id="link"]').send_keys(track_url)
        sleep(1)
        driver.find_element_by_xpath('//*[@id="load"]').click()

        try:
            wait = WebDriverWait(driver, 200)
            wait.until(
                presence_of_element_located(
                    (By.XPATH, '//div[contains(@class, "download-card")]')
                )
            )
            element_with_string = driver.find_element_by_xpath(
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
            dl_url = driver.find_element_by_xpath(downloadbutton_xpath).get_attribute(
                "href"
            )

            extra_data = {"track_name": track_name, "track_id": track_id,
            "download_url": dl_url, "artist_name": artist_name, "artist_id": artist_id
            }

            print(extra_data)

            # Append data to lists and Save

            song_urls_df = song_urls_df.append(extra_data, ignore_index=True)
            song_urls_df.to_csv(f"track_urls_{n}.csv")

            if verbose:
                print("Got url for: " + track_name)
        except TimeoutError:
            print("Wait period lapsed")
            continue


if __name__ == "__main__":

    scrape_song_urls(verbose=False, csv_name="only_tracks.csv")
