#!/bin/bash

export DEBUG='True'
export DB_USER=tiger
# export DB_PASSWORD='hEPgu1lQz!*F'
export DB_PASSWORD='jkrowling'
export DB_HOST=127.0.0.1
export DB_NAME='crawl'
export SECRET_KEY='f^ls9+#3o_ztn#qk$^9v$j5hbnz)@eyn*l)cwzs7a#lzggn%h2'

export PYTHONPATH=$PWD:$HOME/futile:/var/compiled:$HOME/repos/app_common:$HOME/repos/id_generator:$PYTHONPATH:$HOME/repos/pysvc

export REDIS_IP=127.0.0.1
export REDIS_PORT=6379
export PIKA_IP=127.0.0.1
export PIKA_PORT=9221

export RABBITMQ_IP=127.0.0.1
export RABBITMQ_PORT=5672
export RABBITMQ_USERNAME=tiger
export RABBITMQ_PASSWORD=jkrowling

export INFLUXDB_UDP_PORT=8089
export INFLUXDB_HOST=localhost
export INFLUXDB_DATABASE=crawl

export CONF_PATH=$HOME/repos/conf
python $@
