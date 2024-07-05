# 阶段1: 构建应用程序
FROM python:3.10 AS builder

# 1.1 复制必要文件
WORKDIR /app
COPY requirements.txt /app/
COPY weiruanyahei.ttf /app/
COPY util/* /app/util/
COPY po/* /app/po/
COPY .env /app/
COPY *.py /app/

# 1.2 安装python依赖
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/
RUN pip install --no-cache-dir -r requirements.txt

# 阶段2: 创建轻量级的运行时镜像
FROM python:3.10-slim

# 2.1 复制字体，避免乱码
WORKDIR /usr/share/fonts/chinese/
COPY --from=builder /app/weiruanyahei.ttf /usr/share/fonts/chinese/

# 2.2 复制执行所需的文件
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder  /app/util/* /app/util/
COPY --from=builder  /app/.env /app/
COPY --from=builder  /app/*.py /app/

# 2.3 安装依赖
RUN apt-get update
RUN apt-get install -y libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
libcups2 libdrm2 libdbus-1-3 libxkbcommon0 libatspi2.0-0 libxcomposite1 libxdamage1 \
libxfixes3 libxrandr2 libgbm1 libasound2 libpango-1.0-0 libcairo2
RUN rm -rf /var/lib/apt/lists/*

WORKDIR /app
# 2.4 赋予python.sh/py执行权限, 并创建logs目录
RUN chmod +x /app/main*.py

# 2.5 暴露端口
EXPOSE 8040

# 2.6 运行脚本
# 启动 main_api.py，并将输出重定向到日志文件
CMD python /app/main_api.py
