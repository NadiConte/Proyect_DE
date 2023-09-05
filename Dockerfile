FROM apache/airflow:2.3.3

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install python-dotenv   
RUN pip install pandas 
RUN pip install sqlalchemy 
RUN pip install requests 
RUN pip install datetime 
RUN pip install SMTPEmail
