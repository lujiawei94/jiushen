import json,time
from loguru import logger
import requests
from aiohttp import ClientSession


class HttpHelper:

    @staticmethod
    def send_match_result(data):
        url = "http://apitest.oyechat.club/appapi/match/notify"
        res = requests.post(url, data={'match_ids': f'101,102'})
        print(res.status_code)


    @staticmethod
    async def send_success_userid(userid_tuple):
        if len(userid_tuple) != 2:
            return
        url = "http://apitest.oyechat.club/appapi/match/notify"
        data = {'match_ids':f'{userid_tuple[0]},{userid_tuple[1]}'}
        async with ClientSession() as session:
            async with session.post(url, data=data) as response:
                response = await response.read()

if __name__ == '__main__':
    HttpHelper.send_match_result(1)

