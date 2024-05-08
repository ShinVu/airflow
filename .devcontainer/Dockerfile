FROM apache/airflow

USER root 

RUN apt-get update \
&& apt-get install -y wget \
&& apt-get autoremove -y \ 
&& apt-get clean

USER airflow