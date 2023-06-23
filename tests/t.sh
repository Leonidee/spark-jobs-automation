

yq -i '
    .environ.is_prod = true |
    .environ.type = "dataproc" |
    .logging.level.python = "info" |
    .logging.level.java = "error" 
' /home/ubuntu/code/spark-jobs-automation/config/config.yaml 