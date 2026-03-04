# task-10_3-Final-project  

# NovaData Retail Data Pipeline

Исполнитель: Александр Бурлаков.

Проект демонстрирует построение end-to-end data pipeline для анализа данных розничной сети.

Архитектура реализована с использованием Docker и включает следующие технологии:

- MongoDB
- Kafka
- ClickHouse
- Grafana
- PySpark
- MinIO (S3)

---

# Архитектура проекта

Data Flow:

MongoDB  
↓  
Kafka  
↓  
ClickHouse RAW  
↓  
ClickHouse MART (очистка данных через Materialized View)  
↓  
PySpark ETL (feature engineering)  
↓  
MinIO S3 (экспорт аналитической витрины)

---

# Этапы проекта

## 1 Генерация данных

Скрипт `data_generator.py` генерирует JSON данные:

- товары
- покупатели
- магазины
- покупки

Данные загружаются в MongoDB.

---

## 2 Kafka → ClickHouse RAW

Данные передаются из MongoDB в Kafka и затем загружаются в ClickHouse RAW.

---

## 3 Grafana мониторинг

Создан дашборд, показывающий:

- количество покупок
- количество магазинов
- количество покупателей

### Grafana dashboard

![Grafana Dashboard](screenshots/grafana_dashboard.png)

---

## 4 Очистка данных

SQL-скрипты ClickHouse выполняют:

- удаление дубликатов
- проверку NULL
- проверку пустых значений
- проверку дат
- нормализацию регистра

Очистка реализована через **Materialized View**.

---

## 5 ETL PySpark

PySpark формирует витрину `customer_feature_matrix`.

Создаются признаки поведения клиентов:

- new_customer
- loyal_customer
- recurrent_buyer
- prefers_cash
- prefers_card
- weekend_shopper
- morning_shopper
- night_shopper
- recent_high_spender
- low_cost_buyer
- sum_7d
- sum_30d

---

## 6 Экспорт данных в S3

Финальная витрина экспортируется из ClickHouse в MinIO (S3) в формате CSV.

Файл: analytic_result_2026_02.csv  


Количество столбцов строго соответствует ТЗ.

### S3 файл

![S3 export](screenshots/s3_minio_file.png)

---

# Docker инфраструктура

Проект запускается через docker-compose.

Контейнеры:

- kafka
- zookeeper
- clickhouse
- mongodb
- grafana
- minio

### Docker containers

![Docker containers](screenshots/docker_containers.png)

---

# Alerting

Grafana настроена на обнаружение дубликатов.

Если количество дубликатов превышает 50%, отправляется сообщение в Telegram.

### Telegram alert

![Telegram alert](screenshots/telegram_bot_alert.png)

---

# Feature Matrix

Пример витрины:

![Feature matrix](screenshots/feature_matrix.png)



---
