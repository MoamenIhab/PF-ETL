FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 sqlalchemy geopy pandas pymysql
WORKDIR /app
COPY ./engine ./engine 
COPY . .
CMD [ "python", "./analytics.py" ]