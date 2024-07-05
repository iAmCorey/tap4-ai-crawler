import os
from dotenv import load_dotenv
import logging
from supabase import create_client, Client
from po.user import User
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

    def insert_user(self, user):
        logger.info(f"insert new user: {user}")
        try:
            response = (
                self.client.table("mb_user")
                .insert(user)
                .execute()
            )

            if response.data:
                print("insert suceess:", response.data)
                 # 获取插入的数据
                result = response.data[0]
                
                return result
            else:
                print("insert fail:", response)
                return None
        except Exception as e:
            logger.error(f"DB Insert error", e)
            return None
        
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
                print("query suceess:", response.data)
                 # 获取查询的数据
                result = response.data[0]
                
                return result
            else:
                print("query fail:", response)
                return None
        except Exception as e:
            logger.error(f"DB Insert error", e)
            return None


    def insert_submit_site(self, submit_site):
        logger.info(f"insert submit site: {submit_site}")
        try:
            response = (
                self.client.table("submit_site")
                .insert(submit_site)
                .execute()
            )
            logger.info(f"insert res: {e}")
            return response
        except Exception as e:
            logger.error(f"DB Insert error", e)
            return None
