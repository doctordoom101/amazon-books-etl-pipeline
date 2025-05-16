
# Amazon Books ETL Pipeline

-----------
![alt text](/design-pipeline.jng)


## Create a virtual environment and activate it (optional)
"""

    python -m venv venv
    source venv/bin/activate

"""


# 🔗Important links and Code
-----

## Install Airflow 
-----

Follow steps in the link to setup airflow - https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

## Install PGAdmin 
-----
Code to add in yaml file 


"""

    postgres:
      image: postgres:13
      environment:
        POSTGRES_USER: airflow
        POSTGRES_PASSWORD: airflow
        POSTGRES_DB: airflow
      volumes:
        - postgres-db-volume:/var/lib/postgresql/data
      healthcheck:
        test: ["CMD", "pg_isready", "-U", "airflow"]
        interval: 10s
        retries: 5
        start_period: 5s
      restart: always
      ports:
       - "5432:5432"
  
    pgadmin:
      container_name: pgadmin4_container2
      image: dpage/pgadmin4  
      restart: always  
      environment:
        PGADMIN_DEFAULT_EMAIL: admin@admin.com
        PGADMIN_DEFAULT_PASSWORD: root  
      ports:
       - "5050:80"
       
"""
