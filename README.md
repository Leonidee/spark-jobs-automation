[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

# About

![schema](https://github.com/leonidee/spark-jobs-automation/blob/main/images/schema.png?raw=true)

The goal of this project is to implement an approach for ad-hoc scheduling of Spark jobs, with minimal infrastructure deployment costs.

A pipeline has been built to process the data, which is orchestrated using Airflow. The code in Airflow runs every day on a schedule, sends a request to the Yandex Rest API to start the Hadoop cluster, and waits for a response about the state of the cluster.

A Rest API service has been deployed inside the cluster using Fast API and Uvicorn, with its state being managed by the Unix utility [supervisor](https://github.com/Supervisor/supervisor). When the cluster is launched, supervisor starts up the Uvicorn in the cluster to receive requests from Airflow.

Once the infrastructure deployment is successful, Airflow sends a request to the Cluster to submit the Spark job. Spark processes the data and saves the results to S3 data warehouse. After processing is complete, the cluster API sends a summary of the results to Airflow.

After receiving a response from the Cluster, regardless of the success of the job execution, Airflow sends a request to the Yandex API to stop the cluster.

# Technology and Architecture

The process orchestrator is [Airflow](https://github.com/apache/airflow), deployed in Docker containers using `docker compose`.

A Rest API service, implemented using the [Fast API](https://github.com/tiangolo/fastapi) and [Uvicorn](https://github.com/encode/uvicorn) frameworks, is used to receive requests in the cluster.

[Apache Spark](https://github.com/apache/spark), deployed on a lightweight Hadoop cluster consisting only of a Master node and several compute nodes (without HDFS), is the primary data processing engine.

S3 is the main data warehouse, providing flexibility and cost-effectiveness for data storage.

All cloud infrastructure provided by [Yandex Cloud](https://cloud.yandex.com/en-ru/).

# Project structure

![project-tree](https://github.com/leonidee/spark-jobs-automation/blob/main/images/project-tree.png?raw=true)


This project structure appears to be organized into several directories and files, including:

- `api`: Contains the files necessary to run the Restful API service in Hadoop cluster.

    Including `api.py` and `run-api.sh`.
    Second used by `supervisor` service to auto-deploy API on each cluster start.

- `config`: Contains main configuration files for Airflow deployment and Spark jobs submitting.

- `dags`: This directory contains the main DAG file `datamart-collector-dag.py`, which defines the workflow in Airflow.

- `jobs`: Contains all jobs files for Spark execution.

- `src`: Is the main source code directory of the project, splited into several subdirectories.

    Each directory contains a separate business entity and its own logic.

- `supervisor`: Contains the configuration file for the `supervisor`.

- `templates`: Project tamplates, including `.env.template` for required environment variables and `docstring-template.mustache` for geneting docstring for all Project's python object in same style.

- `tests`: This directory contains the unit tests for the project, organized into several subdirectories in the same manner as the main folders.

    All modules tested with built-in `unittest` and `pytest` libraries, except for Spark modules in `src/spark/`, due to the features of how Spark application works.
    
    Spark modules was tested manually in Cluster. Maybe this process will be somehow automated in future versions.

- `utils`: This directory contains bash scripts for setting up the project's environment.

# Deploy and usage

`./utils/setup-project` - is a main entry point for configuring a new environment required to deploy a project.

The project requires three main environments: a Dataproc cluster, host with Airflow and some kind of development or testing environment, which can be either of them.

After running this script, you will be prompted to choose which environment you want to set up. And all of the required preperation will be done automatically.

**Note**: To deploy project you need a ready-to-use Dataproc Cluster by any provider you want. I used [Yandex Data Proc](https://cloud.yandex.com/en-ru/services/data-proc).