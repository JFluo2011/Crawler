import os
import logging
import sys
from asyncio.windows_events import ProactorEventLoop
import asyncio
import json

from crawler import Crawler
from common.utils import setup_log


def run():
    setup_log(logging.INFO, os.path.join(os.path.abspath('.'), 'logs', 'look_ua.log'))
    source_urls = [
        ('https://www.look.com.ua/love/page/{}/', 42),
        # ('https://www.look.com.ua/spring/page/{}/', 94),
        # ('https://www.look.com.ua/autumn/page/{}/', 99),
        # ('https://www.look.com.ua/hi-tech/page/{}/', 114),
        # ('https://www.look.com.ua/summer/page/{}/', 119),
        # ('https://www.look.com.ua/newyear/page/{}/', 156),
        # ('https://www.look.com.ua/men/page/{}/', 157),
        # ('https://www.look.com.ua/holidays/page/{}/', 159),
        # ('https://www.look.com.ua/creative/page/{}/', 168),
        # ('https://www.look.com.ua/winter/page/{}/', 172),
        # ('https://www.look.com.ua/situation/page/{}/', 172),
        # ('https://www.look.com.ua/music/page/{}/', 184),
        # ('https://www.look.com.ua/food/page/{}/', 211),
        # ('https://www.look.com.ua/weapon/page/{}/', 217),
        # ('https://www.look.com.ua/aviation/page/{}/', 261),
        # ('https://www.look.com.ua/textures/page/{}/', 267),
        # ('https://www.look.com.ua/minimalism/page/{}/', 278),
        # ('https://www.look.com.ua/movies/page/{}/', 280),
        # ('https://www.look.com.ua/3d/page/{}/', 286),
        # ('https://www.look.com.ua/abstraction/page/{}/', 293),
        # ('https://www.look.com.ua/space/page/{}/', 302),
        # ('https://www.look.com.ua/sport/page/{}/', 307),
        # ('https://www.look.com.ua/mood/page/{}/', 422),
        # ('https://www.look.com.ua/flowers/page/{}/', 595),
        # ('https://www.look.com.ua/macro/page/{}/', 636),
        # ('https://www.look.com.ua/travel/page/{}/', 674),
        # ('https://www.look.com.ua/fantasy/page/{}/', 687),
        # ('https://www.look.com.ua/anime/page/{}/', 694),
        # ('https://www.look.com.ua/games/page/{}/', 720),
        # ('https://www.look.com.ua/other/page/{}/', 778),
        # ('https://www.look.com.ua/animals/page/{}/', 1103),
        # ('https://www.look.com.ua/landscape/page/{}/', 1140),
        # ('https://www.look.com.ua/nature/page/{}/', 1142),
        # ('https://www.look.com.ua/auto/page/{}/', 1559),
        # ('https://www.look.com.ua/girls/page/{}/', 9266),
    ]
    if sys.platform == 'win32':
        loop = ProactorEventLoop()
    else:
        loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    redis_key = 'look_ua'
    crawler = Crawler(redis_key, max_tasks=1000, store_path='D:\\download\\')
    for info in source_urls:
        for i in range(1, info[1] + 1):
            json_data = {
                'url': info[0].format(i),
                'type_': 'text',
                'operate_func': 'parse_detail_task',
            }
            crawler.insert_task(json.dumps(json_data))

    try:
        loop.run_until_complete(crawler.crawl())  # Crawler gonna crawl.
    except KeyboardInterrupt:
        sys.stderr.flush()
        logging.warning('\nInterrupted\n')
    finally:
        loop.run_until_complete(crawler.close())
        loop.stop()
        loop.run_forever()

        loop.close()


def main():
    run()


if __name__ == '__main__':
    main()

