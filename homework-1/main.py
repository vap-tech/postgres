"""
Скрипт для заполнения данными таблиц в БД Postgres.

# Самый быстрый способ на мой взгляд, однако не демонстрирует навыков делать запросы
from sqlalchemy import create_engine

data = pd.read_csv(r'./north_data/customers_data.csv')
engine = create_engine(f'postgresql+psycopg2://postgres:{user_input}@localhost/north')
data.to_sql("customers", engine, if_exists="append")

"""
import psycopg2
import pandas as pd

# Берём пароль из stdin
user_input = input('Введите пароль пользователя postgres ')

# Создаем подключение
conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password=user_input
)

# Создаём курсор
cur = conn.cursor()

# Case 1

# Читаем данные из файла
data = pd.read_csv(r'./north_data/customers_data.csv')

for a, b in data.iterrows():

    cur.execute("INSERT INTO customers VALUES(%s, %s, %s)", (b.values[0], b.values[1], b.values[2]))

# Коммит
conn.commit()


# Case 2

# Читаем данные из файла
data = pd.read_csv(r'./north_data/employees_data.csv')

for a, b in data.iterrows():
    c = b.values
    cur.execute("INSERT INTO employees VALUES(%s, %s, %s, %s, %s, %s)", (c[0], c[1], c[2], c[3], c[4], c[5]))

# Коммит
conn.commit()

# Case 3

# Читаем данные из файла
data = pd.read_csv(r'./north_data/orders_data.csv')

for a, b in data.iterrows():
    c = b.values
    cur.execute("INSERT INTO orders VALUES(%s, %s, %s, %s, %s)", (c[0], c[1], c[2], c[3], c[4]))

# Коммит
conn.commit()

# Закрыли курсор
cur.close()

# Закрыли соединение
conn.close()
