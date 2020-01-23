#!/bin/bash

###### Start to Install libraries

#sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
#sudo pip-3.6 install pandas==0.20.3
#sudo pip-3.6 install scikit-learn==0.19.1

###### End to Install libraries


###### Create bash script

## path in emr
PATH_FILES="/home/hadoop"

## optional parameters 'qa,prd...'
BUCKET_S3=$1
ENVIRONMENT=$2

## create sh
$(echo "touch $PATH_FILES/spark_jobs.sh")

echo "#!/bin/bash
BUCKET_S3=\$1
ENVIRONMENT=\$2
SCRIPT_NAME=\$3

COMMAND=\"spark-submit --packages=org.apache.hadoop:hadoop-aws:2.7.3,org.postgresql:postgresql:9.4.1211,com.databricks:spark-xml_2.10:0.4.1 s3://\$BUCKET_S3/\$ENVIRONMENT/scripts/\$SCRIPT_NAME\"

eval \$COMMAND

" >> "$PATH_FILES/spark_jobs.sh"


###### Create cron_config
echo "*/5 * * * * $PATH_FILES/spark_jobs.sh $BUCKET_S3 $ENVIRONMENT first_etl.py >> $PATH_FILES/batchjobs.log 2>&1" >> "$PATH_FILES/cron_config

$(echo "crontab $PATH_FILES/cron_config")

exit 0
