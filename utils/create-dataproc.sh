#!/usr/bin/env bash
#
# Creates Data Proc cluster in Yandex Cloud infra.
#

yc dataproc cluster create de-dataproc-06 \
   --bucket=de-dataproc-bucket \
   --zone=ru-central1-c \
   --service-account-name=leonide \
   --version=2.1 \
   --services=SPARK,YARN \
   --ssh-public-keys-file=/Users/leonidgrisenkov/.ssh/id_rsa.pub \
   --subcluster name="master",`
               `role=masternode,`
               `resource-preset=s3-c2-m8,`
               `disk-type=network-hdd,`
               `disk-size=20,`
               `subnet-name=default-ru-central1-c,`
               `assign-public-ip=true \
   --subcluster name="compute",`
               `role=computenode,`
               `resource-preset=s3-c4-m16,`
               `disk-type=network-hdd,`
               `disk-size=20,`
               `subnet-name=default-ru-central1-c,`
               `hosts-count=3,`
               `assign-public-ip=false \
   --security-group-ids=enp9qq7b5fn7f20erdcg \
   --deletion-protection=false
