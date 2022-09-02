import csv
import json
import os
import random
import threading
import time
import traceback
from datetime import datetime

import requests
from bs4 import BeautifulSoup

ffl = "https://www.ffldealernetwork.com"
url = f"{ffl}/wapi/widget"

payload = {
    'dc_id': '1',
    'header_type': 'html',
    'request_type': 'POST',
    'currentPage': '1',
    'profsPost': '{"new_filename":"search_results"}',
    'widget_name': 'Add-On - Bootstrap Theme - Search - Lazy Loader'
}
fieldnames = ["License Region", "Business Name", "Membership Plan","Logo", "Visit Website", "Online Social Profiles", "Phone Number",
              "Location", "Street", "City", "State", "ZIP", "Country", "License Expiration Date", "FFL Number",
              "View my FFL", "Year Established", "Business Description", "URL"]
encoding = 'utf8'
scraped = []
thread_count = 10
semaphore = threading.Semaphore(thread_count)
blocked = False


def scrape(href):
    global blocked
    print(f"Working on {href}")
    soup = BeautifulSoup(requests.get(href).text, 'lxml')
    # with open('index.html') as f:
    #     soup = BeautifulSoup(f.read(), 'lxml')
    data = {
        "URL": href,
        "Logo": soup.find('meta', {'property': "og:image"})['content'],
        "Membership Plan": soup.find('button', {'data-activefavorite':True})['data-activefavorite'],
    }
    try:
        data["Business Description"] = \
            soup.find_all('h2', {"class": "tmargin tpad xs-text-center xs-center-block clearfix"})[
                -1].find_next_sibling('p').text
    except:
        data["Business Description"] = ""
    count = 0
    for div in soup.find_all('div', {"class": "table-view-group clearfix"}):
        li = div.find_all('li')
        if len(li) < 2:
            continue
        else:
            count += 1
        if "Show Phone Number" in li[1].text:
            try:
                data[li[0].text.strip()] = li[1].find("u").text.strip()
            except:
                data[li[0].text.strip()] = li[1].text.strip().split()[-1]
        elif "Location" in li[0].text:
            data['Location'] = li[1].text.strip()
            span = li[1].find_all('span')
            if len(span) == 1:
                data['State'] = span[0].text.strip()
            else:
                offset = 4 - len(span)
                if offset == 0:
                    data['Street'] = span[0].text.strip()
                else:
                    data["Street"] = ""
                data['City'] = span[1 - offset].text.strip()
                data['State'] = span[2 - offset].text.strip()
                data['ZIP'] = span[3 - offset].text.strip()
                try:
                    data['Country'] = str(li[1]).split('<br/>')[2].split("<")[0].strip()
                except:
                    data['Country'] = "United States"
        elif "Online Social Profiles" in li[0].text:
            data['Online Social Profiles'] = " | ".join(
                [f"{a['title']}: {a['href']}" for a in li[1].find_all('a')])
        elif "Request Information" in li[1].text:
            pass
        elif "FFL" == li[0].text.strip():
            data['View my FFL'] = li[1].find('a')['href']
        elif li[0].text.strip() in fieldnames:
            data[li[0].text.strip()] = li[1].text.strip()
    if count == 0:
        print(f"No data found for {href}")
        blocked = True
    else:
        print(json.dumps(data, indent=4))
        append(data)


def getData(href):
    global blocked
    with semaphore:
        try:
            scrape(href)
        except:
            traceback.print_exc()
            print(f"Error on {href}")
            with open("errors.txt", "a") as f:
                f.write(f"{href}\n")


def append(data):
    with open('ffldealernetwork.csv', 'a', newline='', encoding=encoding) as f:
        csv.DictWriter(f, fieldnames).writerow(data)
    scraped.append(data['URL'])


def main():
    global blocked
    logo()
    print("Loading...")
    if not os.path.isfile("ffldealernetwork.csv"):
        with open('ffldealernetwork.csv', 'w', newline='', encoding=encoding) as f:
            csv.DictWriter(f, fieldnames).writeheader()
    else:
        with open('ffldealernetwork.csv', 'r', newline='', encoding=encoding) as f:
            reader = csv.DictReader(f)
            for row in reader:
                scraped.append(row['URL'])
    # getData(f"{ffl}/united-states/chandler/western/2a-ballistic-solutions")
    soup = BeautifulSoup(requests.get(f"{ffl}/search_results").text, 'lxml')
    total = int(soup.find('span', {'class': "total__js"}).text.replace(",", ""))
    print(f"Total listings: {total}")
    print(f"Total pages {int(total / 10) + 1}")
    threads = []
    start = 0
    if os.path.isfile("page.txt"):
        with open("page.txt") as pfile:
            start = pfile.read()
        print(f"Resuming from page {start}")
    for i in range(int(start), int(total / 10) + 1):
        print(f"Working on page {i}")
        payload['currentPage'] = str(i)
        soup = BeautifulSoup(requests.post(url, data=payload).text, 'lxml')
        for div in soup.find_all('div', {'class': "grid_element"}):
            href = f'{ffl}{div.find("a")["href"]}'
            if href.startswith('https://www.ffldealernetwork.com/pro/'):
                print(f"Not valid listing {href}")
            elif href in scraped:
                print(f"Already scraped {href} ")
            else:
                while blocked:
                    time.sleep(random.randint(1, 5))
                    try:
                        scrape(href)
                        blocked = False
                        print(f"IP Unblocked!! {datetime.now()}")
                        break
                    except:
                        print(f"IP Blocked {datetime.now()}")
                t = threading.Thread(target=getData, args=(href,))
                threads.append(t)
                t.start()
        for t in threads:
            t.join()
        with open('page.txt', 'w') as pfile:
            pfile.write(str(i))


def logo():
    print(r"""
______ ______  _      ______              _               _   _        _                          _    
|  ___||  ___|| |     |  _  \            | |             | \ | |      | |                        | |   
| |_   | |_   | |     | | | | ___   __ _ | |  ___  _ __  |  \| |  ___ | |_ __      __ ___   _ __ | | __
|  _|  |  _|  | |     | | | |/ _ \ / _` || | / _ \| '__| | . ` | / _ \| __|\ \ /\ / // _ \ | '__|| |/ /
| |    | |    | |____ | |/ /|  __/| (_| || ||  __/| |    | |\  ||  __/| |_  \ V  V /| (_) || |   |   < 
\_|    \_|    \_____/ |___/  \___| \__,_||_| \___||_|    \_| \_/ \___| \__|  \_/\_/  \___/ |_|   |_|\_\
========================================================================================================
                    ffldealernetwork.com scraper by github.com/evilgenius786
========================================================================================================
[+] Multithreaded
[+] Browserless
[+] CSV / JSON output
[+] Resumable
________________________________________________________________________________________________________
""")


if __name__ == '__main__':
    while True:
        try:
            main()
            print("Done")
        except:
            print("Retrying...")

