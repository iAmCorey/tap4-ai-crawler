import asyncio
import logging
import os
import threading
import requests
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from website_crawler import WebsitCrawler
from po.user import User
import uuid
from util.db_util import DBUtil
import time

from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone, timedelta

app = Flask(__name__)
website_crawler = WebsitCrawler()
# load_dotenv()


db = DBUtil()

# 指定读取 .env.local 文件
load_dotenv(dotenv_path='.env.local')  
auth_secret = 'Bearer ' + os.getenv('AUTH_SECRET')

# 设置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 定义时区，使用 UTC+8（例如中国时区）
tz_utc_8 = timezone(timedelta(hours=8))


### api

###  user service


@app.route('/userService/register', methods=['POST'])
def register():
    logger.info("------------------ register -----------------")
    

    reqStr = request.get_json()
    logger.info(f"reqStr: {reqStr}")

    auth_header = request.headers.get('Authorization')
    if not auth_header:
        return jsonify({'error': 'Authorization is required'}), 400

    if auth_secret != auth_header:
        return jsonify({'error': 'Authorization is error'}), 400

    if not reqStr:
        return deal_with_response({
            'code': 0,
            'msg': 'request data is required'
        }), 400
    

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(db.register(reqStr))

    # result = db.register(reqStr)


    logger.info(f"register result: {result}")

    return deal_with_response(result)




### site service 

# crawl site and store in 'mb_site' table
@app.post('/siteService/crawlSite')
def crawl_site():
    logger.info("------------------ crawlSite -----------------")

    reqStr = request.get_json()
    logger.info(f"reqStr: {reqStr}")

    if not reqStr:
        return deal_with_response({
            'code': 0,
            'msg': 'request data is required'
        }), 400
    

    url = reqStr.get('url')
   
    auth_header = request.headers.get('Authorization')

    if not url:
        return jsonify({'error': 'URL is required'}), 400

    if not auth_header:
        return jsonify({'error': 'Authorization is required'}), 400

    if auth_secret != auth_header:
        return jsonify({'error': 'Authorization is error'}), 400
    

    response = crawl_site_handle(reqStr)

    return deal_with_response(response)

    


def crawl_site_handle(reqStr: str = None):
    url = reqStr.get('url')
    tags = reqStr.get('tags')  # tag数组
    languages = reqStr.get('languages')  # 需要翻译的多语言列表
    submit_by = reqStr.get('submit_by') # 提交人

    # 是否使用已有缓存 默认-是
    use_cache = reqStr.get('use_cache') if reqStr.get('use_cache') is not None else 1 

    auth_header = request.headers.get('Authorization')

    if use_cache and use_cache == 1:
        # use cache
        logger.info('use cache')
        exist_site = db.get_site(url)
        if exist_site:
            logger.info(f"site {url} exist")
            return {
                'code': 100,
                'msg': 'success',
                'data': exist_site
            }
        

    loop = asyncio.get_event_loop()
    crawl_result = loop.run_until_complete(website_crawler.scrape_website(url.strip(), tags, languages))

    logger.info(f"crawling result: {crawl_result}")

    # 若result为None,则 code="10001"，msg="处理异常，请稍后重试"
    code = 200
    msg = 'success'
    if crawl_result is None:
        code = 10001
        msg = 'fail'


    # 插入到site表中
    # insert_result = db.insert_site(crawl_result)

    if submit_by:
        crawl_result['submit_by'] = submit_by

    insert_result = db.upsert_site(crawl_result)
    logger.info(f"upsert result: {insert_result}")


    # 将数据映射到 'data' 键下
    response = {
        'code': code,
        'msg': msg,
        'data': crawl_result
    }
    return response


# submit todo site
@app.post('/siteService/submitSite')
def submit_site():
    logger.info("------------------ submitSite -----------------")
    

    reqStr = request.get_json()
    logger.info(f"reqStr: {reqStr}")

    url = reqStr.get('url')
    if not url:
        return jsonify({'error': 'URL is required'}), 400


    auth_header = request.headers.get('Authorization')
    if not auth_header:
        return jsonify({'error': 'Authorization is required'}), 400

    if auth_secret != auth_header:
        return jsonify({'error': 'Authorization is error'}), 400

    if not reqStr:
        return deal_with_response({
            'code': 0,
            'msg': 'request data is required'
        }), 400
    

    result = db.insert_submit_site(reqStr)


    logger.info(f"insert result: {result}")

    # 若result为None,则 code="10001"，msg="处理异常，请稍后重试"
    code = 200
    msg = 'success'
    if result is None:
        code = 10001
        msg = 'insert fail'


    # 将数据映射到 'data' 键下
    response = {
        'code': code,
        'msg': msg,
        'data': result
    }
    return deal_with_response(response)



# get all todo site
@app.post('/siteService/getTodoSite')
def get_todo_site():
    logger.info("------------------ getTodoSite -----------------")
    

    reqStr = request.get_json()
    logger.info(f"reqStr: {reqStr}")

    auth_header = request.headers.get('Authorization')
    if not auth_header:
        return jsonify({'error': 'Authorization is required'}), 400

    if auth_secret != auth_header:
        return jsonify({'error': 'Authorization is error'}), 400

    if not reqStr:
        return deal_with_response({
            'code': 0,
            'msg': 'request data is required'
        }), 400
    

    result = db.get_todo_site(reqStr)


    logger.info(f"query result: {result}")

    # 若result为None,则 code="10001"，msg="处理异常，请稍后重试"
    code = 200
    msg = 'success'
    if result is None:
        code = 10001
        msg = 'no data'


    # 将数据映射到 'data' 键下
    response = {
        'code': code,
        'msg': msg,
        'data': result
    }
    return deal_with_response(response)

# crawl todo site
@app.post('/siteService/crawlTodoSite')
def crawl_todo_site():
    logger.info("------------------ crawlTodoSite -----------------")
    
    
    reqStr = request.get_json()
    logger.info(f"reqStr: {reqStr}")

    auth_header = request.headers.get('Authorization')
    if not auth_header:
        return jsonify({'error': 'Authorization is required'}), 400

    if auth_secret != auth_header:
        return jsonify({'error': 'Authorization is error'}), 400

    if not reqStr:
        return deal_with_response({
            'code': 0,
            'msg': 'request data is required'
        }), 400
    
    response = crawl_todo_site_handle(reqStr)

    return deal_with_response(response)


def crawl_todo_site_handle(reqStr):

    todo_site_list = db.get_todo_site(reqStr)

    # 若result为None,则 code="10001"，msg="处理异常，请稍后重试"
    code = 200
    msg = 'success'
    if todo_site_list is None:
        return {
            'code': 10001,
            'msg': 'no data'
        }


    # crawl these site and store to mb_site
    result_list = []
    success = 0
    total = len(todo_site_list)
    logger.info(f"todo site list: {total}")


    start_time = time.time()  # 记录开始时间

    for todo_site in todo_site_list:
        this_result = {}
        this_result['id'] = todo_site['id']
        if todo_site['url']:
            this_result['url'] = todo_site['url']
            logger.info(f"crawl submit site: {todo_site['url']}")

            handle_res = crawl_site_handle(todo_site)
            
            if handle_res['code'] and handle_res['code'] > 0:
                # crawl success, update submit_site's status
                
                update_res = db.update_todo_to_done_by_url(todo_site['url'])
                logger.info(f"update {todo_site['url']} to done res: {update_res}")
                
                if update_res:
                    response = {
                        'code': 200,
                        'msg': 'success'
                    } 
                    success += 1
                else:
                    response = {
                        'code': 0,
                        'msg': 'update submit_site status'
                    }
                
                
            else:
                response = {
                    'code': 0,
                    'msg': 'crawl handle fail'
                }


        else:
            # 将数据映射到 'data' 键下
            response = {
                'code': 0,
                'msg': 'url data is required'
            }
        this_result['response'] = response
        result_list.append(this_result)

    final_response = {
        'code': code,
        'msg': msg,
        'result': result_list
    }

    end_time = time.time()    # 记录结束时间
    elapsed_time = end_time - start_time  # 计算耗时

    logger.info(f'crawl todo site job done. total: {total}, success: {success}. spend time: {elapsed_time:.4f} s')


    return final_response


### ---------------- original api ----------------


@app.route('/site/crawl', methods=['POST'])
def scrape():
    data = request.get_json()
    url = data.get('url')
    tags = data.get('tags')  # tag数组
    languages = data.get('languages')  # 需要翻译的多语言列表

    auth_header = request.headers.get('Authorization')

    if not url:
        return jsonify({'error': 'URL is required'}), 400

    if not auth_header:
        return jsonify({'error': 'Authorization is required'}), 400

    if auth_secret != auth_header:
        return jsonify({'error': 'Authorization is error'}), 400

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(website_crawler.scrape_website(url.strip(), tags, languages))

    # 若result为None,则 code="10001"，msg="处理异常，请稍后重试"
    code = 200
    msg = 'success'
    if result is None:
        code = 10001
        msg = 'fail'

    # 将数据映射到 'data' 键下
    response = {
        'code': code,
        'msg': msg,
        'data': result
    }
    return jsonify(response)


@app.route('/site/crawl_async', methods=['POST'])
def scrape_async():
    data = request.get_json()
    url = data.get('url')
    callback_url = data.get('callback_url')
    key = data.get('key')  # 请求回调接口，放header Authorization: 'Bear key'
    tags = data.get('tags')  # tag数组
    languages = data.get('languages')  # 需要翻译的多语言列表

    auth_header = request.headers.get('Authorization')

    if not url:
        return jsonify({'error': 'url is required'}), 400

    if not callback_url:
        return jsonify({'error': 'call_back_url is required'}), 400

    if not auth_header:
        return jsonify({'error': 'Authorization is required'}), 400

    if auth_secret != auth_header:
        return jsonify({'error': 'Authorization is error'}), 400

    loop = asyncio.get_event_loop()

    # 创建线程，传递参数
    t = threading.Thread(target=async_worker, args=(loop, url, tags, languages, callback_url, key))
    # 启动线程
    t.start()

    # 若result为None,则 code="10001"，msg="处理异常，请稍后重试"
    code = 200
    msg = 'success'

    # 将数据映射到 'data' 键下
    response = {
        'code': code,
        'msg': msg
    }
    return jsonify(response)


def async_worker(loop, url, tags, languages, callback_url, key):
    # 爬虫处理封装为一个异步任务
    result = loop.run_until_complete(website_crawler.scrape_website(url.strip(), tags, languages))
    # 通过requests post 请求调用call_back_url， 携带参数result， heaer 为key
    try:
        logger.info(f'callback begin:{callback_url}')
        response = requests.post(callback_url, json=result, headers={'Authorization': 'Bearer ' + key})
        if response.status_code != 200:
            logger.error(f'callback error:{callback_url}',response.text)
        else:
            logger.info(f'callback success:{callback_url}')
    except Exception as e:
        logger.error(f'call_back exception:{callback_url}',e)


def deal_with_response(response):
    logger.info(f'return response: {response}')
    logger.info('-----------------------------------------')
    return jsonify(response)



### cron
# 初始化调度器
scheduler = BackgroundScheduler()

# test cron
def test_cron():
    logger.info(f"测试定时任务运行中... 当前时间: {datetime.now(tz_utc_8)}")


# 跑submit_site的cron
def submit_site_cron():
    logger.info(f"跑submit_site定时任务运行中... 当前时间: {datetime.now(tz_utc_8)}")
    # 每次跑100条
    reqStr = {
        "limit": 100
    }

    crawl_todo_site_handle(reqStr)

# 5分钟跑一次
scheduler.add_job(test_cron, 'interval', minutes=5)

# 2小时跑一次
scheduler.add_job(submit_site_cron, 'interval', hours=1)



if __name__ == '__main__':
    # 启动定时任务调度器
    scheduler.start()
    try:
        asyncio.run(app.run(host='0.0.0.0', port=8040, threaded=False))
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        scheduler.shutdown()

