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



###  user service


@app.route('/api/register', methods=['POST'])
def register():
    logger.info("------------------ register -----------------")
    

    reqStr = request.get_json()
    logger.info(f"reqStr: {reqStr}")

    if not reqStr:
        return deal_with_response({
            'code': 0,
            'msg': 'request data is required'
        }), 400
    

    name = reqStr.get('name') if reqStr.get('name') else ""
    email = reqStr.get('email') if reqStr.get('email') else ""
    pwd = reqStr.get('pwd') if reqStr.get('pwd') else ""
    mobile = reqStr.get('mobile') if reqStr.get('mobile') else ""
    source = reqStr.get('source') if reqStr.get('source') else ""
    thumbnail_url =  reqStr.get('thumbnail_url') if reqStr.get('thumbnail_url') else ""


    if not email:
        return deal_with_response({
            'code': 0,
            'msg': 'email is required'
        }), 400
    
    # check if email is register
    exist_user = db.get_user(email)
    if exist_user:
        logger.warn(f"email {email} exist")
        return deal_with_response({
            'code': 0,
            'msg': 'email is registered'
        }), 400
    
    new_user = {
            "user_id": str(uuid.uuid4()), # uuid
            "name": name,
            "email": email,
            "mobile": mobile,
            "pwd": pwd,
            "thumbnail_url": thumbnail_url,
            "source": source,
            "status": 1,
            "type": 0,
            
    }

    logger.info(f"adding user: {new_user}")
    result = db.insert_user(new_user)

    logger.info(f"adding result: {result}")

    # 若result为None,则 code="10001"，msg="处理异常，请稍后重试"
    code = 200
    msg = 'success'
    if result is None:
        code = 10001
        msg = 'fail'


    logger.info(f"user_id: {result['user_id']}")
    #  返回user_id
    dataJs = {}
    if 'user_id' in result and result['user_id'] is not None:
        dataJs['user_id'] = result['user_id']

    # 将数据映射到 'data' 键下
    response = {
        'code': code,
        'msg': msg,
        'data': dataJs
    }
    return deal_with_response(response)




### site service


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
    logger.info('-------------------------------')
    return jsonify(response)

if __name__ == '__main__':
    asyncio.run(app.run(host='0.0.0.0', port=8040, threaded=False))
