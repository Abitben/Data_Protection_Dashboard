# Data Protection Dashboard

## Intro

Hi ! When I was a IT jurist in a law firm, I wish I had a tool that can enable a global vision on data protection in France and Europe. Back then, if I could anticipate the decisions of the supervisory authorities, gain insight into the legal basis for fines, the amount of the fines, where the inspections where located, in which countries my conformity is the most at risk, I might have prioritize my work for my client in a different manner.

After completing my data analyst training, I decided to create this tool and dashboard : [link here]( 
https://lookerstudio.google.com/u/0/reporting/d08ba2d8-551d-4e1d-a194-4d2029af13b4/page/p_jo0423kk8c)

This is my first solo project, and it marks my initial foray into various technologies, such as Docker and Astronomer's Airflow, which are useful for integrating dbt lineage within Airflow.
## Languages and tools used for this project

For orchestration :
- [Astronomer CLI (Airflow) with Docker](https://docs.astronomer.io/astro/cli/overview) 

For sourcing and ingestion :
- Python
- Pandas
- Selenium with Selenium Grid (Warning : The Selenium Docker Image was deployed on a MacOs M1 system, it might not work on other system)

For transforming :
- dbt
- Python
- Google Dataproc (for dbt models) 

For storage :
- Google Big Query

For Visualization :
- Looker Studio 

## How to use 

As it uses Astronomer CLI, you might install it on your setup. You also need Docker to make it work.

The initial sourcing and ingestion has been done before this orchestration. It is designed to update and transform the data from the various sources.  This orchestration needs a GCP project of your own to make it work. 
