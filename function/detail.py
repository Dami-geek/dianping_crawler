# -*- coding:utf-8 -*-

"""
      ┏┛ ┻━━━━━┛ ┻┓
      ┃　　　　　　 ┃
      ┃　　　━　　　┃
      ┃　┳┛　  ┗┳　┃
      ┃　　　　　　 ┃
      ┃　　　┻　　　┃
      ┃　　　　　　 ┃
      ┗━┓　　　┏━━━┛
        ┃　　　┃   神兽保佑
        ┃　　　┃   代码无BUG！
        ┃　　　┗━━━━━━━━━┓
        ┃CREATE BY SNIPER┣┓
        ┃　　　　         ┏┛
        ┗━┓ ┓ ┏━━━┳ ┓ ┏━┛
          ┃ ┫ ┫   ┃ ┫ ┫
          ┗━┻━┛   ┗━┻━┛

"""

from bs4 import BeautifulSoup

from utils.get_font_map import get_search_map_file
from utils.requests_utils import requests_util
from utils.spider_config import spider_config


class Detail():
    def __init__(self):
        self.is_ban = False

    def get_detail(self, shop_id, request_type='proxy, cookie', last_chance=False):
        dishes_info_VO=[]
        dishes_info=[]
        if self.is_ban and spider_config.USE_COOKIE_POOL is False:
            print('详情页请求被ban，程序继续运行')
            return_data = {
            '推荐菜': '',
            '全部菜':''
        }
            return return_data
        url = f"https://www.dianping.com/ajax/json/shopDynamic/shopTabs?shopId={shop_id}"
        r = requests_util.get_requests(url, request_type=request_type)
        # 给一次retry的机会，如果依然403则判断为被ban
        if r.status_code == 403:
            if last_chance is True:
                self.is_ban = True
            return self.get_detail(shop_id=shop_id, request_type=request_type, last_chance=True)
        try:
            res = requests_util.get_requests(url, request_type=request_type).json()
            allDishes = res['allDishes']
            dishesWithPicVO = res['dishesWithPicVO']
            for dish in allDishes:
                dishTagName = dish['dishTagName']
                finalPrice = dish['finalPrice']
                # 将数据转换为字符串并添加到列表中
                dishes_info.append(f"{dishTagName},{finalPrice}")

            # 处理 dishesWithPicVO
            for dish in dishesWithPicVO:
                dishTagName = dish['dishTagName']
                finalPrice = dish['finalPrice']
                # 将数据转换为字符串并添加到列表中
                dishes_info_VO.append(f"{dishTagName},{finalPrice}")
        except:
            pass
        return {
            '推荐菜': dishes_info_VO,
            '全部菜':dishes_info
        }
