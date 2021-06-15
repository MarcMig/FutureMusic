import pandas as pd
from time import sleep
import json
from multi_webbing import multi_webbing as mw
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support.expected_conditions import presence_of_element_located

def scrape_song_urls(n = 'all', threads = 1, verbose=True):

    # Get list song Urls from DataFrame
    song_urls = pd.read_csv('tracks_and_beats.csv', usecols=["track_name","track_url"])
    song_urls_df = []

    if n != 'all': song_urls = song_urls.iloc[:n,]

    # Initiate threads
    url = 'https://loader.to/en3/soundcloud-downloader.html'
    num_threads = threads
    my_threads = mw.MultiWebbing(num_threads, web_module="selenium")
    my_threads.start()


    #  Cannot append to Pandas in Job function so worker functions are defined
    #  to create dataFrame and save data  

    def add_to_data():
        df_data = pd.DataFrame(song_urls_df)
        df_data.to_csv(f'song_urls_{n}.csv')
    

    # Job function for multi-threading 

    def get_dl_url(job):
        
        # Retrieve local vars

        track_name = job.custom_data[0]
        track_url = job.custom_data[1]
        song_urls_df = job.custom_data[2]

        # Go to webpage, find elements

        job.get_url()
        sleep(1)

        job.driver.find_element_by_xpath('//*[@id="format"]/optgroup[1]/option[8]').click()
        job.driver.find_element_by_xpath('//*[@id="link"]').send_keys(track_url)
        job.driver.find_element_by_xpath('//*[@id="load"]').click()
        sleep(30)
        button = job.driver.find_element_by_id('ds')
        ele = button.find_elements_by_tag_name('a')
        dl_url = ele[2].get_attribute('href')

        print(dl_url)
        extra_data = {track_name: dl_url}

        # Append data to lists and Save

        job.lock.acquire()

        song_urls_df.append(extra_data) 
        add_to_data()

        job.lock.release()

        if verbose: print('Got url for: ' + track_name)

    # Loop adds jobs to queue
    for track_name,track_url in [tuple(x) for x in song_urls.to_numpy()]:
        my_threads.job_queue.put(mw.Job(get_dl_url, url, (track_name, track_url, song_urls_df)))

    # While loop adds 
    while my_threads.job_queue.qsize() > 0:
        sleep(1)
        if verbose: print(my_threads.job_queue.qsize())

    my_threads.finish()

if __name__ == '__main__':

    scrape_song_urls(n=10, threads=1)
