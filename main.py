import os
import csv
from tqdm import tqdm
from function.search import Search
from function.detail import Detail
from utils.spider_config import spider_config

# 实例化 Detail 和 Search 类
d = Detail()
s = Search()

def get_search_url(cur_page, city_id, keyword='炸鸡'):
    """
    获取搜索链接
    @param cur_page: 当前页码 / Current page number
    @param city_id: 城市ID / City ID
    @param keyword: 搜索关键字 / Search keyword
    @return: 拼接好的搜索URL和一些需要的选项 / Constructed search URL and request type
    """
    # 使用简单的字符串拼接构建URL / Construct URL using simple string concatenation
    base_url = f'http://www.dianping.com/search/keyword/{city_id}/0_{keyword}/p'

    # 根据页码构建搜索URL / Construct search URL based on page number
    if cur_page == 1:
        return f"{base_url}1", 'proxy, cookie'
    else:
        return f"{base_url}{cur_page}", 'proxy, cookie'

def initialize_csv(file_path, fieldnames):
    """
    初始化 CSV 文件，写入表头
    Initialize the CSV file and write the header
    :param file_path: CSV 文件路径 / CSV file path
    :param fieldnames: 表头字段名列表 / List of header field names
    """
    file_exists = os.path.isfile(file_path)
    with open(file_path, mode='w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        print(f'CSV 文件 "{file_path}" 已创建并写入表头。')

def save_to_csv(file_path, data, fieldnames):
    """
    将数据写入 CSV 文件
    Write data to the CSV file
    :param file_path: CSV 文件路径 / CSV file path
    :param data: 要写入的数据列表，每个元素为字典 / List of data dictionaries to write
    :param fieldnames: 表头字段名列表 / List of header field names
    """
    with open(file_path, mode='a', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        for row in data:
            writer.writerow(row)
    print(f'已将 {len(data)} 条数据写入 CSV 文件。')

def main():
    # 定义 CSV 文件路径
    csv_file = 'salad_taiwan.csv'

    # 初始化一个集合来存储所有字段名
    fieldnames_set = set()

    # 开始爬取
    for page in tqdm(range(1, spider_config.NEED_SEARCH_PAGES + 1), desc='搜索页数'):
        search_url, request_type = get_search_url(page, spider_config.LOCATION_ID, spider_config.KEYWORD)
        search_res = s.search(search_url, request_type)
        
        if not search_res:
            print(f'第 {page} 页没有搜索结果，停止爬取。')
            break

        print(f'第 {page} 页搜索结果：{search_res}')

        # 初始化一个列表来存储当前页的所有数据
        page_data = []

        for each_search_res in tqdm(search_res, desc='详细爬取', leave=False):
            try:
                # 爬取推荐菜
                shop_id = each_search_res.get('店铺id')
                if not shop_id:
                    print('未找到店铺ID，跳过该条记录。')
                    continue

                # 获取店铺详情
                each_detail_res = d.get_detail(shop_id)
                if not each_detail_res:
                    print(f'店铺ID {shop_id} 的详情获取失败，跳过。')
                    continue

                # 合并搜索结果和详情结果
                combined_res = {**each_search_res, **each_detail_res}
                page_data.append(combined_res)

                # 更新字段名集合
                fieldnames_set.update(combined_res.keys())

            except Exception as e:
                print(f'处理店铺ID {shop_id} 时发生错误: {e}')
                continue

        # 如果这是第一次获取字段名，初始化 CSV 文件
        if page == 1:
            # 可选择在此处手动排序 fieldnames
            fieldnames = list(fieldnames_set)
            initialize_csv(csv_file, fieldnames)

        # 将当前页的数据写入 CSV
        if page_data:
            save_to_csv(csv_file, page_data, fieldnames)

        # 如果当前页的结果少于预期数量，可能表示没有更多数据，停止爬取
        if len(search_res) < 15:
            print(f'第 {page} 页的数据少于预期，停止爬取。')
            break

if __name__ == "__main__":
    main()