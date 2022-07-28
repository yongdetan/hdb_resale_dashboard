# HDB Resale Dashboard

![HDB_Resale_Dashboard-1](https://user-images.githubusercontent.com/61530179/181432539-bb092445-556d-4928-ab2e-4bf545e4b655.png)

Link to dashboard
- https://datastudio.google.com/reporting/a57df325-96e8-4ae0-8eac-76c5f23ece87/page/NPQxC

# Overview

This dashboard allows users to visualize the prices of resale flats in Singapore and analyse the Singapore Overnight Rate Average (SORA) rate of which will be the new benchmark interest rate for home loans by 2024.

All data presented in this dashboard is sourced from Google BigQuery. The data that is loaded into BigQuery is retrieved through a data pipeline that extracted data from two different API sources, MAS's Domestic Interest 
Rate API and data.gov.sg's Resale Flat Prices API. Data that were extracted from the APIs were loaded into to Google Cloud Storage which serves as a data lake to store all raw and transformed data. Once the pipeline has finished
processing the raw data with PySpark, the transformed data in Google Cloud Storage is then loaded into the data into BigQuery.

To automate and ensure that the data is always updated Apache Airflow is used to orchestrate all of the tasks within the pipeline. Docker is also used containerized the entire pipeline to ensure reproducibility. Lastly, since this 
dashboard uses cloud services such as Google Cloud Storage and Google BigQuery to function as a Data Lake and a Data Warehouse respectively, Terraform is used to build and manage these two sevices.

# Motivation

There are two main motivations as to why I started out this project

- The first reason is because I wanted to learn more about data engineering and the technologies associated with it. This is also the biggest reason why I used tools such as Google Cloud Platform and PySpark to transform and load the data data when I could simply use a local database and pandas.
- The second reason is because I feel that owning a house is something every one would like to have in the future. As such, building a pipeline to supply data for a dashboard that allow people to analyse the interest rate for home loan and housing prices would be a good problem statement.

# Architecture

![Architecture](https://user-images.githubusercontent.com/61530179/180378899-fd990169-caf5-474e-ad30-6630b47ff5c4.png)


# Metadata

Sora
| Field name | Type | Description |
| ------------- | ------------- | ------------- |
|  sora  | float | Singapore Overnight Rate Average (SORA) |
| sora_index | float | Singapore Overnight Rate Average (SORA) Index |
| end_of_day | date | Date which the Singapore Overnight Rate Average (SORA) was released |
| comp_1m_sora | float | 1-month Compounded Singapore Overnight Rate Average (SORA) |
| comp_3m_sora | float | 3-month Compounded Singapore Overnight Rate Average (SORA) |
| comp_6m_sora | float | 6-month Compounded Singapore Overnight Rate Average (SORA) |

HDB  
| Field name | Type | Description |
| ------------- | ------------- | ------------- |
| month | date | Date (month and year) of transaction |
| town | string | Town |
| flat_type | string | Flat type |
| block | string | Flat block number |
| street_name | string | Flat street name |
| storey_range | string | Flat storey range |
| floor_area_sqm | float | Flat floor area (sqm) |
| flat_model | string | Flat model |
| lease_commence_date | integer | Flat lease commence date |
| remaining_lease | string | Flat remaining lease |
| resale_price | float | Flat resale price |
| maturity | string | Flat maturity |
| region | string | Flat region |
