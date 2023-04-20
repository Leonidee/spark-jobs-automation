

# Pipeline

![pipeline](https://github.com/Leonidee/spark-jobs-automation/blob/master/images/pipeline.png?raw=true)


# Supervisor

Install supervisor Debian/Ubuntu:

```shell
sudo apt install -y supervisor
```

Copy supervisor config to config folder:

```shell
sudo cp ./supervisor/api.conf /etc/supervisor/conf.d/api.conf
```
And run:

```shell
sudo service supervisor start
```

Or restart if service already running:

```shell
sudo service supervisor restart
```

