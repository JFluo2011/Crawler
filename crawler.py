import os
import re
import json
import random
import socket
import asyncio
from asyncio import Queue
import logging
import concurrent

import aiohttp
import async_timeout
import redis
from lxml import etree


class Crawler(object):
    def __init__(self, redis_key, max_tasks=100, store_path='.', *, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.redis_key = redis_key
        self.max_tasks = max_tasks
        self.current_coro = 0
        self.store_path = store_path
        # conn = aiohttp.TCPConnector(
        #     family=socket.AF_INET,
        #     verify_ssl=False,
        # )
        self.session = aiohttp.ClientSession(loop=self.loop)
        # self.session = aiohttp.ClientSession(connector=conn, loop=self.loop)
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
        }
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    async def crawl(self):
        while True:
            # if len(asyncio.Task.all_tasks()) >= self.max_tasks:
            if self.current_coro >= self.max_tasks:
                await asyncio.sleep(1)
                continue
            await self.create_task()

    async def create_task(self):
        task, json_data = await self.get_task()
        if task is None:
            await asyncio.sleep(10)
        else:
            asyncio.ensure_future(self.do_task(json_data))
            self.current_coro += 1

    async def do_task(self, json_data):
        url = json_data['url']
        type_ = json_data['type_']
        operate_func = json_data['operate_func']
        source = await self.fetch(url, json_data, type_=type_)
        if source is not None:
            await getattr(self, operate_func)(source, json_data)
        self.current_coro -= 1

    async def parse_detail_task(self, source, json_data):
        selector = etree.HTML(source)
        for sel in selector.xpath('//*[@class="gallery_image"]'):
            xpath_ = './/img[@class="img-responsive img-rounded"]/@src'
            category = re.findall('ua/(.*?)/page', json_data['url'])[0]
            image_dir, image_number = re.findall('/mini/(\d+)/(\d+)\.jpg', sel.xpath(xpath_)[0])[0]
            meta = {
                'url': sel.xpath('./@href')[0],
                'image_number': image_number,
                'image_dir': image_dir,
                'category': category,
                'type_': 'text',
                'operate_func': 'parse_download_task',
            }
            # self.loop.run_in_executor(None, self.insert_task, json.dumps(meta))
            self.insert_task(json.dumps(meta))

    async def parse_download_task(self, source, json_data):
        base_url = 'https://look.com.ua/pic'
        selector = etree.HTML(source)
        for url in selector.xpath('//*[@class="llink list-inline"]/li/a/@href'):
            resolution = re.findall(r'download/\d+/(\d+x\d+)/', url)[0]
            path = os.path.join(os.path.abspath(self.store_path), 'images', json_data['category'],
                                json_data['image_number'], resolution + '.jpg')
            url = '/'.join([base_url, json_data['image_dir'], resolution,
                            'look.com.ua-' + json_data['image_number'] + '.jpg'])
            if os.path.exists(path):
                logging.info('image {} already downloaded'.format(path))
                continue
            meta = {'url': url, 'path': path, 'operate_func': 'download_image', 'type_': 'content'}
            # self.loop.run_in_executor(None, self.insert_task, json.dumps(meta))
            self.insert_task(json.dumps(meta))

    async def download_image(self, source, json_data):
        # self.loop.run_in_executor(None, self.save_image, json_data['path'], source)
        # self.save_image(json_data['path'], source)
        pass

    async def fetch(self, url, json_data, type_='text'):
        # async with self.download_semaphore:
        # logging.info('active tasks count: {}'.format(len(asyncio.Task.all_tasks())))
        try:
            async with async_timeout.timeout(30):
                async with self.session.get(url, headers=self.headers, ssl=False, timeout=30, allow_redirects=False,
                                            proxy=self.get_proxy()) as response:
                    return await response.read() if type_ == 'content' else await response.text()
        except Exception as err:
            if isinstance(err, concurrent.futures._base.TimeoutError) or isinstance(err, asyncio.TimeoutError):
                pass
                logging.warning('{} raised TimeoutError'.format(url))
            else:
                pass
                logging.warning('{} raised {}'.format(url, str(err)))
            # task = asyncio.Task.current_task()
            # if not task.done():
            #     task.cancel()
            # self.loop.run_in_executor(None, self.insert_task, json.dumps(json_data))
            # self.insert_task(json.dumps(json_data))
            return None

    @staticmethod
    def save_image(path, content):
        if not os.path.exists('\\'.join(path.split('\\')[:-1])):
            os.makedirs('\\'.join(path.split('\\')[:-1]))
        with open(path, 'wb', encoding='utf-8') as f:
            f.write(content)
        logging.info('{}: downloaded'.format(path))

    async def get_task(self):
        task = {
            'url': 'http://localhost:8080/1',
            'type_': 'text',
            'operate_func': 'download_image',
        }
        return task, task
        # task = self.redis_client.spop(self.redis_key)
        # return task, (task is not None) and json.loads(task)

    def insert_task(self, task):
        self.redis_client.sadd(self.redis_key, task)

    def get_proxy(self):
        try:
            proxy = random.choice(self.redis_client.keys('http://*'))
        except:
            return None
        return proxy

    async def close(self):
        await self.session.close()


def main():
    pass


if __name__ == '__main__':
    main()
