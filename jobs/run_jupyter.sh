#!/bin/sh

# wait for 10s
sleep 10

mkdir -p /data/notebooks

# start service
echo "Starting jupyter" >> /data/logs/jupyter-notebook.log
/usr/local/bin/jupyter notebook --NotebookApp.notebook_dir='/data/notebooks'  --ip=0.0.0.0 \
    --allow-root >> /data/logs/jupyter-notebook.log &

# show token
sleep 10
/usr/local/bin/jupyter notebook list >> /data/logs/jupyter-notebook.log &
