# airflow_etl
up and running
docker compose down && docker compose up -d

airflow webserver UI  http://0.0.0.0:8080/home (user: airflow, password: airflow)
to check dag list on webserver with cli or ui
docker exec -it airflow_lab_etl-airflow-webserver-1 bash
airflow tasks list <dagid> 

the output files is to find in scheduler, not webserver!!!
docker exec -it airflow_lab_etl-airflow-scheduler-1 bash

new dag 
define a new dag in the dags folder of the project, then restart the scheduler, it is sync with it, no manuel copy !!!
![Bildschirmfoto 2025-07-10 um 9 26 06 AM](https://github.com/user-attachments/assets/88c55a4a-d9fb-4f96-834b-8251121545e9)
![Bildschirmfoto 2025-07-09 um 10 19 08 PM](https://github.com/user-attachments/assets/ea25f7e4-f5e5-423b-bcda-82ea53a8f499)


input/output file
need to change the access right in the tmp folder (created with mkdir) on scheduler
![Bildschirmfoto 2025-07-09 um 10 11 55 PM](https://github.com/user-attachments/assets/0dd9f7cc-b508-42a8-bff2-dd9a00cf582f)

Airflow database dump
/Users/xingliu/my_python_stuff/coursera/ibm_data_engineer/capstone_project_13/Module5_ETL_and_data_pipelines/lab/airflow_backup.sql

capstone_local.py is for the local testing of the capstone13 module5 assigment part 2 

process_weg_log.py is the capstone13 module5 assigment.


