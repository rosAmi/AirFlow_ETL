FROM apache/airflow:3.1.7

ADD requirements.txt .
# copy requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
