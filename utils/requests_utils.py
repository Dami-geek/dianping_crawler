import os
import sys
import time
import json
import requests
from tqdm import tqdm
from faker import Factory

from utils.cache import cache
from utils.config import global_config
from utils.get_file_map import get_map
from utils.spider_config import spider_config


class RequestsUtils():
    """
    Request utility class used to handle all request-related operations,
    including global anti-ban sleep mechanisms.
    
    请求工具类，用于处理所有与请求相关的操作，包括全局防封禁的休眠机制。
    """

    def __init__(self):
        # Retrieve configuration settings
        # 获取配置设置
        requests_times = spider_config.REQUESTS_TIMES  # Format: '100,5;200,10' (request count, sleep time)
        self.cookie = spider_config.COOKIE             # User-defined cookie
        self.ua = spider_config.USER_AGENT             # User-defined User-Agent
        self.ua_engine = Factory.create()              # Faker instance for generating random User-Agents

        # Ensure User-Agent is not empty
        # 确保 User-Agent 不为空
        if self.ua is None:
            print('User-Agent cannot be empty.')
            print('User-Agent 不能为空。')
            sys.exit()

        # Determine if a cookie pool is being used
        # 判断是否使用 cookie 池
        self.cookie_pool = spider_config.USE_COOKIE_POOL

        # Determine if a proxy is being used
        # 判断是否使用代理
        self.ip_proxy = spider_config.USE_PROXY
        if self.ip_proxy:
            self.proxy_pool = []  # Initialize proxy pool if proxies are used
            # 如果使用代理，则初始化代理池

        # Parse the request times for implementing sleep intervals
        # 解析请求次数以实现休眠间隔
        try:
            self.stop_times = self.parse_stop_time(requests_times)
        except:
            print('Error parsing requests_times in configuration. Please ensure correct input format (use English punctuation).')
            print('配置文件中的 requests_times 解析错误。请确保输入格式正确（使用英文标点）。')
            sys.exit()
        self.global_time = 0  # Initialize global request counter
        # 初始化全局请求计数器

    def create_dir(self, file_name):
        """
        Create a directory if it does not exist.
        创建目录，如果不存在的话。
        
        :param file_name: Name or path of the directory to create.
        :param file_name: 要创建的目录的名称或路径。
        """
        if not os.path.exists(file_name):
            os.mkdir(file_name)
            # 创建目录
            print(f'Directory "{file_name}" created.')
            # 输出目录创建信息
        else:
            print(f'Directory "{file_name}" already exists.')
            # 输出目录已存在的信息

    def parse_stop_time(self, requests_times):
        """
        Parse the requests_times configuration to determine when to implement sleep intervals.
        解析 requests_times 配置以确定何时实现休眠间隔。
        
        :param requests_times: A string with format 'request_count,sleep_time;...'
                              格式为 '请求次数,休眠时间;...' 的字符串。
        :return: A list of [request_count, sleep_time] pairs.
                 返回一个包含 [请求次数, 休眠时间] 对的列表。
        """
        each_stop = requests_times.strip(';').split(';')  # Remove trailing semicolon and split
        # 移除末尾的分号并进行分割
        stop_time = []
        for i in range(len(each_stop) - 1, -1, -1):
            stop_time.append(each_stop[i].split(','))  # Split each pair and add to the list
            # 分割每一对并添加到列表中
        return stop_time  # Returns a list in reverse order for priority handling
        # 返回一个反向的列表以便优先处理

    def get_requests(self, url, request_type):
        """
        Make an HTTP GET request based on the specified request_type.
        根据指定的 request_type 发起 HTTP GET 请求。
        
        :param url: The URL to request.
                    要请求的 URL。
        :param request_type: The type of request, affecting headers and proxy usage.
                             请求类型，影响请求头和代理的使用。
        :return: The response object after handling verification if necessary.
                 在必要时处理验证后的响应对象。
        """
        # Ensure request_type is valid
        # 确保 request_type 是有效的
        valid_request_types = [
            'no header', 
            'no proxy, cookie', 
            'no proxy, no cookie', 
            'proxy, no cookie', 
            'proxy, cookie'
        ]
        assert request_type in valid_request_types, f"Invalid request_type: {request_type}"
        # 如果 request_type 无效，则抛出断言错误

        # Requests without headers (e.g., font file downloads) are not counted in the anti-ban statistics
        # 不带请求头的请求（例如字体文件下载）不计入防封禁统计
        if request_type == 'no header':
            r = requests.get(url=url)
            return r
            # 直接发起请求并返回响应

        # For requests using the local IP (no proxy)
        # 对于使用本地 IP（无代理）的请求
        if 'no proxy' in request_type:
            self.freeze_time()  # Implement anti-ban sleep if necessary
            # 实现防封禁休眠（如果需要）

            if request_type == 'no proxy, no cookie':
                # Make a request without cookies
                # 发起不带 cookie 的请求
                r = requests.get(url, headers=self.get_header(cookie=None, need_cookie=False))

            elif request_type == 'no proxy, cookie':
                # Make a request with cookies
                # 发起带 cookie 的请求
                cur_cookie = self.get_cookie(url)
                r = requests.get(url, headers=self.get_header(cookie=cur_cookie, need_cookie=True))

            # Handle verification if required
            # 如有需要，处理验证
            return self.handle_verify(r=r, url=url, request_type=request_type)

        # For requests that can use a proxy
        # 对于可以使用代理的请求
        if request_type == 'proxy, no cookie':
            if self.ip_proxy:
                # Attempt to make a request using a proxy
                # 尝试使用代理发起请求
                try:
                    r = requests.get(
                        url, 
                        headers=self.get_header(cookie=None, need_cookie=False), 
                        proxies=self.get_proxy(), 
                        timeout=10
                    )
                except requests.RequestException as e:
                    print(f'Proxy request failed: {e}')
                    # 输出代理请求失败的信息
                    return self.get_requests(url, request_type)  # Retry the request
                    # 重试请求
            else:
                # Fallback to making a request without a proxy
                # 回退到不使用代理发起请求
                r = requests.get(url, headers=self.get_header(cookie=None, need_cookie=False))
            return self.handle_verify(r, url, request_type)

        if request_type == 'proxy, cookie':
            # Implement anti-ban sleep for requests with cookies
            # 对带有 cookie 的请求实现防封禁休眠
            self.freeze_time()

            cur_cookie = self.get_cookie(url)
            header = self.get_header(cookie=cur_cookie, need_cookie=True)

            if self.ip_proxy:
                # Make a request using a proxy and cookies
                # 使用代理和 cookie 发起请求
                try:
                    r = requests.get(
                        url, 
                        headers=header, 
                        proxies=self.get_proxy(), 
                        timeout=10
                    )
                except requests.RequestException as e:
                    print(f'Proxy request failed: {e}')
                    # 输出代理请求失败的信息
                    return self.get_requests(url, request_type)  # Retry the request
                    # 重试请求
            else:
                # Fallback to making a request without a proxy
                # 回退到不使用代理发起请求
                r = requests.get(url, headers=header)

            return self.handle_verify(r, url, request_type)

        # If an unsupported request_type is provided
        # 如果提供了不支持的 request_type
        raise AttributeError(f"Unsupported request_type: {request_type}")
        # 抛出属性错误

    def freeze_time(self):
        """
        Implement anti-ban sleep intervals based on the number of requests made.
        根据发起的请求数量实现防封禁的休眠间隔。
        """
        self.global_time += 1  # Increment the global request counter
        # 增加全局请求计数器
        if self.global_time != 1:
            # Iterate over stop_times to check if a sleep interval should be implemented
            # 遍历 stop_times 以检查是否需要实施休眠间隔
            for each_stop_time in self.stop_times:
                request_count, sleep_duration = int(each_stop_time[0]), int(each_stop_time[1])
                if self.global_time % request_count == 0:
                    # Sleep for the specified duration with slight randomization to mimic human behavior
                    # 以指定的持续时间休眠，并略微随机化以模拟人类行为
                    for _ in tqdm(range(sleep_duration), desc='Global Waiting'):
                        import random
                        sleep_time = 1 + (random.randint(1, 10) / 100)  # Sleep between 1.01 and 1.1 seconds
                        time.sleep(sleep_time)
                    break  # Only the highest priority sleep interval is applied per request
                    # 每次请求仅应用最高优先级的休眠间隔

    def handle_verify(self, r, url, request_type):
        """
        Handle verification (e.g., captcha) if the response indicates such a requirement.
        如果响应指示需要验证（例如验证码），则处理验证。
        
        :param r: The response object.
                  响应对象。
        :param url: The URL that was requested.
                    被请求的 URL。
        :param request_type: The type of request made.
                             发起的请求类型。
        :return: The response object after handling verification.
                 处理验证后的响应对象。
        """
        # Only handle verification here; other status codes (e.g., 403) require different handling
        # 这里只处理验证；其他状态码（例如 403）需要不同的处理
        if 'verify' in r.url:
            # Decide whether to handle verification based on request_type and proxy usage
            # 根据 request_type 和代理使用情况决定是否处理验证
            if request_type != 'proxy, no cookie' or not spider_config.USE_PROXY:
                print('Verification required. Please complete the verification and press Enter to continue:', r.url)
                print('需要验证。请完成验证后按回车继续：', r.url)
                input()
            else:
                print('Verification encountered, skipping handling due to proxy settings.')
                print('遇到验证，由于代理设置，跳过处理。')
            # Retry the request after handling verification
            # 处理验证后重试请求
            return self.get_requests(url, request_type)
        else:
            return r  # Return the response if no verification is required
            # 如果不需要验证，则返回响应

    def get_retry_time(self):
        """
        Get the number of times to retry a request when encountering failures.
        在遇到失败时获取请求的重试次数。
        
        :return: The number of retries allowed.
                 允许的重试次数。
        """
        # Set default retry times based on configuration
        # 根据配置设置默认重试次数
        if spider_config.REPEAT_NUMBER == 0:
            retry_time = 5  # Default to 5 retries if not specified
            # 如果未指定，默认为 5 次重试
        else:
            retry_time = spider_config.REPEAT_NUMBER + 1
            # 否则，重试次数为配置的 REPEAT_NUMBER 加 1
        return retry_time

    def get_request_for_interface(self, url):
        """
        Make a request to an API interface, ensuring a correct and valid response.
        发起对 API 接口的请求，确保响应正确有效。
        
        :param url: The API endpoint URL.
                    API 端点的 URL。
        :return: The valid response object.
                 有效的响应对象。
        """
        retry_time = self.get_retry_time()
        while retry_time > 0:
            retry_time -= 1
            r = self.get_requests(url, request_type='proxy, no cookie')
            try:
                # Parse the response as JSON
                # 将响应解析为 JSON
                r_json = json.loads(r.text)
                if r_json.get('code') == 406:
                    # Handle verification required on first proxy request (cold start)
                    # 处理首次代理请求时需要验证的情况（冷启动）
                    if cache.is_cold_start:
                        print('Verification required. Please complete the verification and press Enter to continue:', r_json['customData']['verifyPageUrl'])
                        print('需要验证。请完成验证后按回车继续：', r_json['customData']['verifyPageUrl'])
                        input()
                        r = self.get_requests(url, request_type='proxy, no cookie')
                        cache.is_cold_start = False
                if r_json.get('code') == 200:
                    # Valid response received
                    # 收到有效响应
                    break
                if retry_time <= 0:
                    print('Please check your tsv and uuid, or the proxy quality may be low.')
                    print('请检查您的 tsv 和 uuid，或者代理质量可能较低。')
                    exit()
            except json.JSONDecodeError:
                # Handle JSON parsing errors
                # 处理 JSON 解析错误
                print('JSON decoding failed. Retrying...')
                print('JSON 解码失败。正在重试...')
                pass
        return r

    def get_cookie(self, url):
        """
        Retrieve the cookie to use for a request.
        获取用于请求的 cookie。
        
        :param url: The URL for which the cookie is needed.
                    需要 cookie 的 URL。
        :return: The cookie string.
                 cookie 字符串。
        """
        # Placeholder for more complex cookie retrieval logic if needed
        # 如果需要，可以在此处添加更复杂的 cookie 获取逻辑
        return self.cookie

    def judge_request_type(self, url):
        """
        Determine the type of request based on the URL, for cookie pool management.
        根据 URL 确定请求类型，用于 cookie 池管理。
        
        :param url: The URL to analyze.
                    要分析的 URL。
        :return: A string indicating the request type ('detail', 'review', 'search').
                 表示请求类型的字符串（'detail', 'review', 'search'）。
        """
        if 'shop' in url:
            return 'detail'
        elif 'review' in url:
            return 'review'
        else:
            return 'search'

    def get_header(self, cookie, need_cookie=True):
        """
        Construct the headers for a request.
        构建请求的头部信息。
        
        :param cookie: The cookie to include in the headers.
                       要包含在头部信息中的 cookie。
        :param need_cookie: Whether a cookie is required in the headers.
                            头部信息中是否需要包含 cookie。
        :return: A dictionary of headers.
                 包含头部信息的字典。
        """
        # Use the user-defined User-Agent or generate a random one
        # 使用用户定义的 User-Agent 或生成一个随机的
        ua = self.ua if self.ua is not None else self.ua_engine.user_agent()

        # Use the provided cookie or the default one
        # 使用提供的 cookie 或默认的 cookie
        cookie = cookie if cookie is not None else self.cookie

        # Construct headers based on whether a cookie is needed
        # 根据是否需要 cookie 构建头部信息
        if need_cookie:
            header = {
                'User-Agent': ua,
                'Cookie': cookie
            }
        else:
            header = {
                'User-Agent': ua,
            }
        return header

    def get_proxy(self):
        """
        Retrieve a proxy to use for a request.
        获取用于请求的代理。
        
        :return: A dictionary containing proxy settings.
                 包含代理设置的字典。
        """
        repeat_nub = spider_config.REPEAT_NUMBER
        # HTTP extraction mode
        # HTTP 提取模式
        if spider_config.HTTP_EXTRACT:
            # If the proxy pool is empty, fetch new proxies
            # 如果代理池为空，则获取新的代理
            if len(self.proxy_pool) == 0:
                proxy_url = spider_config.HTTP_LINK
                try:
                    r = requests.get(proxy_url)
                    r_json = r.json()
                    # Adjust parsing based on the structure of the JSON response
                    # 根据 JSON 响应的结构调整解析方式
                    for proxy in r_json:
                        # Add each proxy multiple times based on repeat_nub for reuse
                        # 根据 repeat_nub 多次添加每个代理以便重复使用
                        for _ in range(repeat_nub):
                            self.proxy_pool.append([proxy['ip'], proxy['port']])
                except requests.RequestException as e:
                    print(f'Failed to fetch proxies: {e}')
                    print(f'获取代理失败：{e}')
                    sys.exit()

            # Retrieve a proxy from the pool
            # 从池中获取一个代理
            ip, port = self.proxy_pool.pop(0)
            proxies = self.http_proxy_utils(ip, port)
            return proxies

        # Key extraction mode
        # 秘钥提取模式
        elif spider_config.KEY_EXTRACT:
            proxies = self.key_proxy_utils()
            return proxies

        else:
            print('When using proxies, you must choose either HTTP extraction or key extraction mode.')
            print('使用代理时，必须选择 HTTP 提取或秘钥提取模式之一。')
            sys.exit()
        pass

    def http_proxy_utils(self, ip, port):
        """
        Construct the proxies dictionary for HTTP proxy mode.
        为 HTTP 代理模式构建代理字典。
        
        :param ip: The proxy IP address.
                   代理 IP 地址。
        :param port: The proxy port.
                     代理端口。
        :return: A dictionary containing proxy settings.
                 包含代理设置的字典。
        """
        proxyMeta = f"http://{ip}:{port}"
        proxies = {
            "http": proxyMeta,
            "https": proxyMeta
        }
        return proxies

    def key_proxy_utils(self):
        """
        Construct the proxies dictionary for key-based proxy mode.
        为基于秘钥的代理模式构建代理字典。
        
        :return: A dictionary containing proxy settings.
                 包含代理设置的字典。
        """
        proxyMeta = f"http://{spider_config.KEY_ID}:{spider_config.KEY_KEY}@{spider_config.PROXY_HOST}:{spider_config.PROXY_PORT}"
        proxies = {
            "http": proxyMeta,
            "https": proxyMeta,
        }
        return proxies

    def replace_search_html(self, page_source, file_map):
        """
        Replace encrypted codes in the HTML page source based on font file mappings for search pages.
        根据搜索页面的字体文件映射，替换 HTML 页面源代码中的加密代码。
        
        :param page_source: The original HTML content.
                            原始的 HTML 内容。
        :param file_map: A mapping of font file identifiers to file paths.
                         字体文件标识符到文件路径的映射。
        :return: The modified HTML content with decrypted text.
                 修改后的 HTML 内容，包含解密的文本。
        """
        for font_id, font_path in file_map.items():
            font_map = get_map(font_path)  # Retrieve character mappings from the font file
            # 从字体文件中获取字符映射
            for code_point, character in font_map.items():
                key = str(code_point).replace('uni', '&#x')
                key = f'"{font_id}">{key};'
                value = f'"{font_id}">{character}'
                page_source = page_source.replace(key, value)
                # 替换加密代码为解密字符
        return page_source

    def replace_review_html(self, page_source, file_map):
        """
        Replace encrypted codes in the HTML page source based on font file mappings for review pages.
        根据评论页面的字体文件映射，替换 HTML 页面源代码中的加密代码。
        
        :param page_source: The original HTML content.
                            原始的 HTML 内容。
        :param file_map: A mapping of font file identifiers to file paths.
                         字体文件标识符到文件路径的映射。
        :return: The modified HTML content with decrypted text.
                 修改后的 HTML 内容，包含解密的文本。
        """
        for font_id, font_path in file_map.items():
            font_map = get_map(font_path)
            for code_point, character in font_map.items():
                key = str(code_point).replace('uni', '&#x')
                key = f'"{code_point}"><'
                value = f'"{code_point}">{character}<'
                page_source = page_source.replace(key, value)
                # 替换加密代码为解密字符
        return page_source

    def replace_json_text(self, json_text, file_map):
        """
        Replace encrypted codes in JSON text based on font file mappings.
        根据字体文件映射，替换 JSON 文本中的加密代码。
        
        :param json_text: The original JSON content as a string.
                          原始的 JSON 内容（字符串）。
        :param file_map: A mapping of font file identifiers to file paths.
                         字体文件标识符到文件路径的映射。
        :return: The modified JSON content with decrypted text.
                 修改后的 JSON 内容，包含解密的文本。
        """
        for font_id, font_path in file_map.items():
            font_map = get_map(font_path)
            for code_point, character in font_map.items():
                key = str(code_point).replace('uni', '&#x')
                key = f'\\"{font_id}\\">{key};'
                value = f'\\"{font_id}\\">{character}'
                json_text = json_text.replace(key, value)
                # 替换加密代码为解密字符
        return json_text

    def update_cookie(self):
        """
        Update the cookie from the global configuration.
        从全局配置更新 cookie。
        """
        self.cookie = global_config.getRaw('config', 'Cookie')
        print('Cookie has been updated from the global configuration.')
        print('Cookie 已从全局配置中更新。')


# Instantiate the RequestsUtils class for use
# 实例化 RequestsUtils 类以供使用
requests_util = RequestsUtils()