# fifa-world-cup-2022-prediction-airflow
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
Extract, Transform, Load (ETL) Pipeline using Apache Airflow to scrap Fifa World Cup 2022 matches result.  

## Anggota Kelompok
1. Marsellius 20/456372/TK/50502  
2. Nur Wulan Febriani 20/460557/TK/51146  
3. Pramudya Kusuma Hardika 20/460558/TK/51147  
4. Ilham Faizal Hamka 20/463602/TK/51594  
5. Mufidus Sani 20/463608/TK/51600

## Usage
1. Install WSL, Docker, Docker-Compose, and Apache Airflow (for Windows user)  
2. Clone the Repository  
3. Build docker  
```docker-compose build```
4. Run docker in repository folder directory  
```docker-compose up```
5. Open browser and type  
```localhost:8080```
6. Select and start ```wc_matches_dag```
7. ETL started  
8. CSV generated on ```/data/wc_matches_2022_crawl.csv```
9. PostgresSQL can be check with pgAdmin4 running in ```localhost:4050 (username: pgadmin4@pgadmin4.org password: admin)```  
10. you're done!
