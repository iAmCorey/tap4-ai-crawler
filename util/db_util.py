import os
from dotenv import load_dotenv
import logging
from supabase import create_client, Client
import uuid
from datetime import datetime, timezone, timedelta

# 设置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DBUtil:
    def __init__(self):
        # load_dotenv()
        # 指定读取 .env.local 文件
        load_dotenv(dotenv_path='.env.local')  

        self.supbase_url: str = os.environ.get("SUPABASE_URL")
        self.supbase_key: str = os.environ.get("SUPABASE_KEY")
        self.client: Client = create_client(self.supbase_url, self.supbase_key)
        logger.info(f"supbase url: {self.supbase_url}")

    ### user service
    def get_user(self, email: str = None, user_id: str = None):
        logger.info("get user")
        try:
            query = self.client.table('mb_user').select('*')
            if email:
                logger.info(f"by email: {email}")
                query = query.eq('email', email)
            if user_id:
                logger.info(f"by user_id: {user_id}")
                query = query.eq('user_id', user_id)

            response = query.execute()
            

            if response.data:
                logging.info(f"query success{response.data}")
                 # 获取查询的数据
                result = response.data[0]
                
                return result
            else:
                logging.warning(f"query fail: {response}")
                return None
        except Exception as e:
            logger.error(f"DB Insert error", e)
            return None

    def insert_user(self, user):
        logger.info(f"insert new user: {user}")
        try:
            response = (
                self.client.table("mb_user")
                .insert(user)
                .execute()
            )

            if response.data:
                logging.info(f"insert success: {response.data}")
                 # 获取插入的数据
                result = response.data[0]
                
                return result
            else:
                logging.warning(f"insert fail: {response}")
                return None
        except Exception as e:
            logger.error(f"DB Insert error", e)
            return None
        
    async def register(self, reqStr):
        name = reqStr.get('name') if reqStr.get('name') else ""
        email = reqStr.get('email') if reqStr.get('email') else ""
        pwd = reqStr.get('pwd') if reqStr.get('pwd') else ""
        mobile = reqStr.get('mobile') if reqStr.get('mobile') else ""
        source = reqStr.get('source') if reqStr.get('source') else ""
        thumbnail_url =  reqStr.get('thumbnail_url') if reqStr.get('thumbnail_url') else ""


        if not email:
            return {
                'code': 0,
                'msg': 'email is required'
            }
        
        # check if email is register
        exist_user = self.get_user(email)
        if exist_user:
            logger.warning(f"email {email} exist")
            return {
                'code': 0,
                'msg': 'email is registered'
            }
        
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
        result = self.insert_user(new_user)

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

        return response
        


    ### site service
    # get site from table `site`
    def get_site(self, url: str = None, name: str = None):
        logger.info(f"get site")
        try:
            query = self.client.table('mb_site').select('*')
            if url:
                logger.info(f"by url: {url}")
                query = query.eq('url', url)
            if name:
                logger.info(f"by name: {name}")
                query = query.eq('name', name)
         
            response = query.execute()
            

            if response.data:
                logging.info(f"query success{response.data}")
                 # 获取查询的数据
                result = response.data
                
                return result
            else:
                logging.warning(f"query fail: {response}")
                return None
        except Exception as e:
            logger.error(f"DB Insert error", e)
            return None
        
    def upsert_site(self, site):
        logger.info(f"upsert new site: {site}")

        site['update_time'] = datetime.now().isoformat()

        try:
            response = (
                self.client.table("mb_site")
                .upsert(site)
                .execute()
            )

            if response.data:
                logging.info(f"upsert success: {response.data}")
                 # 获取插入的数据
                result = response.data[0]
                
                return result
            else:
                logging.warning(f"upsert fail: {response}")
                return None
        except Exception as e:
            logger.error(f"DB upsert error", e)
            return None
    
    # insert site to table `site`
    def insert_site(self, site):
        logger.info(f"insert new site: {site}")
        try:
            response = (
                self.client.table("mb_site")
                .insert(site)
                .execute()
            )

            if response.data:
                logging.info(f"insert success: {response.data}")
                 # 获取插入的数据
                result = response.data[0]
                
                return result
            else:
                logging.warning(f"insert fail: {response}")
                return None
        except Exception as e:
            logger.error(f"DB Insert error", e)
            return None
    
     # insert site in table `site` by url
    def update_site_by_url(self, site):
        logger.info(f"insert new site: {site}")
        try:
            response = (
                self.client.table("mb_site")
                .insert(site)
                .execute()
            )

            if response.data:
                logging.info(f"insert success: {response.data}")
                 # 获取插入的数据
                result = response.data[0]
                
                return result
            else:
                logging.warning(f"insert fail: {response}")
                return None
        except Exception as e:
            logger.error(f"DB Insert error", e)
            return None


    # get todo site
    # limit 数量
    def get_todo_site(self, reqStr: str = None):
        logger.info(f"get todo site: {reqStr}")

        limit = reqStr.get('limit')
        # 默认取10个
        if not limit: 
            limit = 10
            
        # 默认按提交时间排序
        order_by = reqStr.get('order_by')
        if not order_by:
            order_by = 'submit_time'
        try:
            query = self.client.table('submit_site').select('*')

            # 选择还未处理的site
            query = query.eq('status', 0)

            # 排序
            if order_by == 'submit_time':
                # 按提交时间排序
                query = query.order("submit_time", desc=True)
            elif order_by == 'priority':
                # 按优先级排序
                query = query.order('priority', desc=True)
            
            # 取固定数量
            query = query.limit(limit)
            response = query.execute()
            

            if response.data:
                logging.info(f"query success: {len(response.data)}")
                 # 获取查询的数据
                result = response.data
                
                return result
            else:
                logging.warning(f"query fail: {response}")
                return None
        except Exception as e:
            logger.error(f"DB Insert error", e)
            return None

    def insert_submit_site(self, submit_site):
        logger.info(f"insert submit site: {submit_site}")

        # 先检查url是否存在
        query = self.client.table('submit_site').select('*').eq('url', submit_site['url'])
        response = query.execute()
        if response.data:
            logger.warning(f'url {submit_site["url"]} exists')
            return None
        try:
            response = (
                self.client.table("submit_site")
                .insert(submit_site)
                .execute()
            )
            logger.info(f"insert res: {response.data}")
            return response.data[0]
        except Exception as e:
            logger.error(f"DB Insert error", e)
            return None

    def update_todo_to_done_by_url(self, url):
        logger.info(f"update todo site to done {url}")

        try:
            response = (
                self.client.table("submit_site")
                .update({"status": 1})
                .eq('url', url)
                .execute()
            )
            logger.info(f"update res: {response.data}")
            return response.data[0]
        except Exception as e:
            logger.error(f"DB update error", e)
            return None
