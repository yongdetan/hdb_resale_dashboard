# Setting up your own Dashboard

## Prerequisites
1. Google Cloud Platform account (free $300 credits for new users): https://cloud.google.com/free
2. Terraform: https://www.terraform.io/
3. Docker: https://www.docker.com/

## Initial Set-Up
Clone the repository
```
git clone https://github.com/yongdetan/hdb_resale_dashboard
```
Change directory to the newly cloned repository
```
cd hdb_resale_dashboard
```
Open Visual Studio Code (or any IDE you are using)
```
code .
```

## Setting up GCP 
1. Create a new project, a new service account (requires permissions for BigQuery and Cloud Storage) within the project, and download the private key (json format) of the account. 
   * refer to this video if you need a guide on how to do it https://www.youtube.com/watch?v=Hajwnmj0xfQ&
2. Once you have the private key ready, move the key to the repository and rename it credentials

## Setting up Terraform
1. Go to variables.tf and rename the following variables.
   * project - rename to your project id
   * region - rename to your closest region
2. Once you have renamed the two variables, open up terminal and go to terraform directory
```
cd hdb_resale_dashboard/terraform
```
3. Once you are inside the folder, you would need to initialize terraform to build the data lake (Google Cloud Storage) and data warehouse (Google BigQuery)
```
terraform init
```
4. To build the data lake and data warehouse, the next step would be to apply all of the code that was set within main.tf (remember to type yes to approve the actions)
```
terraform apply
```
5. ** If you are done with the dashboard remember to destroy the data lake and data warehouse that was created by terraform to prevent charges
```
terraform destroy
```

## Setting up Docker
1. Go to docker-compose.yaml and rename GCS_PROJECT_ID to your project id
2. Once the variable has been renamed, it is time to build the docker image. In the opened terminal, go to airflow directory
```
cd hdb_resale_dashboard/airflow
```
3. Once you are inside airflow directory, build the docker image through the docker-compose yaml file
```
docker-compose build .
```
4. Once the docker image is built, it is time to run airflow and start running the dags!
```
docker-compose up -d
```

## Accessing Airflow Web UI 
1. Start up your browser and go to this link
```
localhost:8080
```
2. The login credentials are as follow:
```
Username: airflow
Password: airflow
```



