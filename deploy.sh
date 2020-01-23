#!/bin/bash

BUCKET_S3=$(grep BUCKET_S3 .env  | cut -d '=' -f 2-)
ENVIRONMENT=$1

if [[ -z $ENVIRONMENT ]]
  then
    echo "You must set the ENVIRONMENT variable (prd, dev)"
  exit 1
fi


aws s3 rm $BUCKET_S3/$ENVIRONMENT --recursive
aws s3 cp --recursive scripts/ $BUCKET_S3/$ENVIRONMENT/scripts --exclude "*.ipynb"
aws s3 cp --recursive bootstrap/ $BUCKET_S3/$ENVIRONMENT/bootstrap
aws s3 cp --recursive data/ $BUCKET_S3/$ENVIRONMENT/data
