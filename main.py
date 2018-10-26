import bz2
import csv
import json
import lzma
import multiprocessing as mp
import os
import sys
from builtins import input
from datetime import datetime
from time import sleep
from urllib.request import urlopen

sys.path.append('modules')

import requests
from future.standard_library import install_aliases
from tqdm import tqdm

from client import DiffbotClient


install_aliases()


URL = 'http://files.pushshift.io/reddit/submissions/'
API_TOKEN = '65d40e5583ad95676f95de1fa1bfbc40'
CPU_COUNT = mp.cpu_count() * 2
manager = mp.Manager()
file_q = manager.Queue()
api_q = manager.Queue()


def download_from_url(url, dst):
    """
    @param: url to download file
    @param: dst place to put the file
    """
    file_size = int(urlopen(url).info().get('Content-Length', -1))
    first_byte = os.path.getsize(dst) if os.path.exists(dst) else 0
    print('{0} {1}'.format(datetime.utcnow()))

    if first_byte >= file_size:
        print('File exists in "downloads" folder. Working with it.')
        return file_size

    print('Downloading file...')
    header = {"Range": "bytes=%s-%s" % (first_byte, file_size)}
    pbar = tqdm(
        total=file_size, initial=first_byte,
        unit='B', unit_scale=True, desc=url.split('/')[-1])
    req = requests.get(url, headers=header, stream=True)

    with(open(dst, 'ab')) as f:
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                pbar.update(1024)

    pbar.close()
    return file_size


def load_bz2_json(filename):
    with bz2.BZ2File(filename, 'r') as f:
        for line in f:
            try:
                j = json.loads(line.decode("utf-8"))
            except json.decoder.JSONDecodeError:
                # print('JSONDecodeError')
                continue
            yield j


def api_worker(worker_number):
    """stupidly simulates long running process"""

    print('Start API {0} worker.'.format(worker_number))

    global file_q
    global api_q
    diffbot = DiffbotClient()

    while True:
        url = api_q.get()

        if url == 'kill':
            print('Kill {0} api_worker'.format(worker_number))
            break

        # print('api_q {0} size {1}'.format(worker_number, api_q.qsize()))

        try:
            response = diffbot.request(url, API_TOKEN, 'article')
        except requests.exceptions.HTTPError:
            continue

        if 'objects' in response:
            obj = response['objects'][0]

            if obj['text'] and obj.get('pageURL') == obj.get('resolvedPageURL'):
                file_q.put(obj)

        # elif 'errorCode' in response:
        #     print('errorCode ', response['errorCode'])
        # else:
        #     print('response ', response)


def file_worker(file_name_csv, fieldnames, category):
    """listens for messages on the q, writes to file."""

    print('Start file worker.')

    global file_q

    with open(file_name_csv, 'a') as csvfile:
        filewriter = csv.DictWriter(csvfile, fieldnames=fieldnames)

        while True:

            obj = file_q.get()

            if obj == 'kill':
                print('Kill file_worker')
                break

            date_str = ''

            if 'date' in obj:
                datetime_object = datetime.strptime(obj['date'], '%a, %d %b %Y %H:%M:%S %Z')
                date_str = datetime_object.strftime('%Y-%m-%d %H:%M:%S')

            filewriter.writerow({
                fieldnames[0]: category,
                fieldnames[1]: obj.get('type', '').replace(',', '","'),
                fieldnames[2]: date_str,
                fieldnames[3]: obj.get('siteName', '').replace(',', '","'),
                fieldnames[4]: obj.get('pageUrl', '').replace(',', '","'),
                fieldnames[5]: obj.get('title', '').replace(',', '","'),
                fieldnames[6]: obj.get('text', '').replace(',', '","'),
            })


def main():
    global file_q
    global api_q
    subreddit_matches = 0
    #fieldnames = ['Internal category name', 'Type', 'Date', 'siteName', 'Url', 'Title', 'Body']
    fieldnames = ['keywords', 'type', 'date', 'source', 'url', 'title', 'text', 'description']
    file_name = input("What is archive name? ")
    in_subreddit = input("What is subreddit name? ")
    category = input("What is category name? ")
    # file_name = 'RS_2011-01.bz2'  # RS_2011-01.bz2 RS_2018-02.xz
    # in_subreddit = 'UXDesign'
    # category = 'UX_Design'

    dst_file = './downloads/{0}'.format(file_name)
    file_name_csv = './downloads/{0}_{1}_{2}'.format(category, in_subreddit, file_name.replace('bz2', 'csv').replace('xz', 'csv'))

    download_from_url('{0}{1}'.format(URL, file_name), dst_file)

    with open(file_name_csv, 'w') as csvfile:
        filewriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
        filewriter.writeheader()

    for i in range(CPU_COUNT):
        api_process = mp.Process(target=api_worker, args=(i,))
        api_process.start()

    file_process = mp.Process(target=file_worker, args=(file_name_csv, fieldnames, category))
    file_process.start()

    sleep(2)  # to do not break progress bar, need to wait workers start

    with open('domain_blacklist.txt', 'r') as blacklist_file:
        blacklist = blacklist_file.read().splitlines()

    file_extension = ''.join(dst_file.split('/')[-1].split('.')[1:])

    if file_extension == 'bz2':
        src_file = load_bz2_json(dst_file)
    elif file_extension == 'xz':
        src_file = lzma.open(dst_file)

    print('Extracting file...')

    for line in src_file:

        if file_extension == 'xz':
            try:
                line = json.loads(line.decode('utf-8'))
            except (TypeError, AttributeError):
                continue

        subreddit = line.get('subreddit')
        url = line.get('url')
        ups = line.get('ups')

        if subreddit and subreddit == in_subreddit and url and ups and ups > 4 and all(blocked_url.lower() not in url.lower() for blocked_url in blacklist):
            subreddit_matches += 1
            api_q.put(url)

    # now we are done, kill the listener
    for _ in range(CPU_COUNT):
        api_q.put('kill')

    # wait when api_q queue will be empty
    while True:
        api_q_size = api_q.qsize()

        sleep(10)
        print('{0} {1}'.format(datetime.utcnow(), api_q_size))

        if not api_q_size:
            file_q.put('kill')  # anly after that we can kill file queue
            break

    print('Task is done!')

    if not subreddit_matches:
        print('No subreddit matches found for a "{0}"'.format(in_subreddit))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        file_q.put('kill')  # to close file correctly
        sleep(3)
        sys.exit(1)
