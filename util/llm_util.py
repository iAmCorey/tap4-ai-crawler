import os
from dotenv import load_dotenv
from groq import Groq
from openai import OpenAI
import logging
from transformers import LlamaTokenizer
from util.common_util import CommonUtil

# 设置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
util = CommonUtil()
# 初始化LLaMA模型的Tokenizer
tokenizer = LlamaTokenizer.from_pretrained("huggyllama/llama-65b")

class LLMUtil:
    def __init__(self):
        load_dotenv()
        self.source = os.getenv('API_SOURCE')
        if self.source == 'openrouter':
            self.groq_api_key = os.getenv('OPENROUTER_API_KEY')
            self.groq_model = os.getenv('OPENROUTER_MODEL')
            self.client = OpenAI(
                base_url="https://openrouter.ai/api/v1",
                api_key=os.getenv("OPENROUTER_API_KEY"),
            )
        elif self.source == 'groq':
            self.groq_api_key = os.getenv('GROQ_API_KEY')
            self.groq_model = os.getenv('GROQ_MODEL')
            self.client = Groq(
                api_key=self.groq_api_key
            )
        self.detail_sys_prompt = os.getenv('DETAIL_SYS_PROMPT')
        self.tag_selector_sys_prompt = os.getenv('TAG_SELECTOR_SYS_PROMPT')
        self.language_sys_prompt = os.getenv('LANGUAGE_SYS_PROMPT')
        self.groq_max_tokens = int(os.getenv('GROQ_MAX_TOKENS', 5000))
        logger.info(f"API source:{self.source}")
        logger.info(f"API Key:{self.groq_api_key[:10]}")
        logger.info(f"using model: {self.groq_model}")
        logger.info(f"max tokens: {self.groq_max_tokens}")
        

    def process_detail(self, user_prompt):
        logger.info("正在处理Detail...")
        return util.detail_handle(self.process_prompt(self.detail_sys_prompt, user_prompt))

    def process_tags(self, user_prompt):
        logger.info(f"正在处理tags...")
        result = self.process_prompt(self.tag_selector_sys_prompt, user_prompt)
        # 将result（逗号分割的字符串）转为数组
        if result:
            tags = [element.strip() for element in result.split(',')]
        else:
            tags = []
        logger.info(f"tags处理结果:{tags}")
        return tags

    def process_language(self, language, user_prompt):
        logger.info(f"正在处理多语言:{language}, user_prompt:{user_prompt}")
        # 如果language 包含 English字符，则直接返回
        if 'english'.lower() in language.lower():
            result = user_prompt
        else:
            result = self.process_prompt(self.language_sys_prompt.replace("{language}", language), user_prompt)
            if result and not user_prompt.startswith("#"):
                # 如果原始输入没有包含###开头的markdown标记，则去掉markdown标记
                result = result.replace("### ", "").replace("## ", "").replace("# ", "").replace("**", "")
        logger.info(f"多语言:{language}, 处理结果:{result}")
        return result

    def process_prompt(self, sys_prompt, user_prompt):
        if not sys_prompt:
            logger.info(f"LLM无需处理，sys_prompt为空:{sys_prompt}")
            return None
        if not user_prompt:
            logger.info(f"LLM无需处理，user_prompt为空:{user_prompt}")
            return None

        logger.info("LLM正在处理")
        try:
            tokens = tokenizer.encode(user_prompt)
            if len(tokens) > self.groq_max_tokens:
                logger.info(f"用户输入长度超过{self.groq_max_tokens}，进行截取")
                truncated_tokens = tokens[:self.groq_max_tokens]
                user_prompt = tokenizer.decode(truncated_tokens)

            chat_completion = self.client.chat.completions.create(
                extra_headers={
                    "HTTP-Referer": os.getenv('SITE_URL'), # Optional, for including your app on openrouter.ai rankings.
                    "X-Title": os.getenv('APP_NAME'), # Optional. Shows in rankings on openrouter.ai.
                },
                messages=[
                    {
                        "role": "system",
                        "content": sys_prompt,
                    },
                    {
                        "role": "user",
                        "content": user_prompt,
                    }
                ],
                model=self.groq_model,
                temperature=1.0,
            )
            if chat_completion.choices[0] and chat_completion.choices[0].message:
                logger.info(f"LLM完成处理，成功响应!")
                return chat_completion.choices[0].message.content
            else:
                logger.info("LLM完成处理，处理结果为空")
                return None
        except Exception as e:
            logger.error(f"LLM处理失败", e)
            return None