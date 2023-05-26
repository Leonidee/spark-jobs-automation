# About

![pipeline](https://github.com/Leonidee/spark-jobs-automation/blob/master/images/pipeline.png?raw=true)

The goal of this project is to implement an approach for ad-hoc scheduling of Spark jobs, with minimal infrastructure deployment costs.

A pipeline has been built to process the data, which is orchestrated using Airflow. The code in Airflow runs every day on a schedule, sends a request to the Yandex Rest API to start the Hadoop cluster, and waits for a response about the state of the cluster.

A Restful service has been deployed inside the cluster using Fast API and Uvicorn, with its state being managed by the Unix utility [supervisor](https://github.com/Supervisor/supervisor). When the cluster is launched, supervisor starts up the Rest API in the cluster to receive requests from Airflow.

Once the infrastructure deployment is successful, Airflow sends a request to the Cluster to submit the Spark job. Spark processes the data and saves the results to S3. After processing is complete, the cluster API sends a summary of the results to Airflow.

After receiving a response from the Cluster, regardless of the success of the job execution, Airflow sends a request to the Yandex API to stop the cluster.

# Technology and Architecture

The process orchestrator is [Airflow](https://github.com/apache/airflow), deployed in Docker containers.

A Rest API service, implemented using the [Fast API](https://github.com/tiangolo/fastapi) and [Uvicorn](https://github.com/encode/uvicorn) frameworks, is used to receive requests in the cluster.

[Apache Spark](https://github.com/apache/spark), deployed on a lightweight Hadoop cluster consisting only of a Master node and several compute nodes (without HDFS), is the primary data processing engine.

S3 is the primary data storage, providing flexibility and cost-effectiveness for data storage.

Cloud infrastructure provided by [Yandex Cloud](https://cloud.yandex.com/en-ru/).

# How to use

*Note*: You must have a Hadoop cluster prepared and configured. The project uses a cloud cluster from Yandex Cloud.

*Note*: And you must also have an S3 baket, the paths to which must be specified in the variables of the `dag.py` file.

*Note*: The project was implemented and tested only on Debian/Ubuntu OS.

All credentials must be located in a `.env` file (you need to create one), which is automatically taken into all Docker containers with Airflow.

In this project we have Hadoop Cluster and Machine (cloud VM or your local machine) with Airflow and to start working we must take the following steps.

Clone the repository:

```shell
git clone https://github.com/Leonidee/spark-jobs-automation.git
```

### Install dependencies

The project is adapted to work with [Poetry](https://github.com/python-poetry/poetry).

Set virtual env in project:

```shell
poetry config virtualenvs.in-project true
```

Install all dependencies listed in `pyproject.toml`:

```shell
poetry install
```

## On Hadoop Cluster Masternode

### Configure Supervisor

Install:

```shell
sudo apt install -y supervisor
```

Copy supervisor config from repository to config folder:

```shell
sudo cp ./supervisor/api.conf /etc/supervisor/conf.d/api.conf
```

And run:

```shell
sudo service supervisor start
```

## On Host with Airflow

### Deploy Airflow

Init airflow:

```shell
docker compose up airflow-init
```

Run containers:

```shell
docker compose up -d
```

UI Airflow will be available at `<your VM host>:8080/`

By default, these credentials are used:
Password: `airflow`
User: `airflow`

You can specify others in `docker-compose.yaml` before deploying.

