# airflow_etl
up and running
docker compose down && docker compose up -d

airflow webserver UI  http://0.0.0.0:8080/home (user: airflow, password: airflow)
to check dag list on webserver with cli or ui
docker exec -it airflow_lab_etl-airflow-webserver-1 bash
airflow tasks list <dagid> 

the output files is to find in scheduler, not webserver!!!
docker exec -it airflow_lab_etl-airflow-scheduler-1 bash



<img width="1448" height="945" alt="airflow_logs" src="https://github.com/user-attachments/assets/bb9b6de9-6f3b-4b26-927b-b921906ce5a3" />

