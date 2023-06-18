FROM apache/airflow:2.6.2-python3.11

USER airflow

COPY ./requirements.txt .

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir -r ./requirements.txt
