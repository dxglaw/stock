#!/bin/sh

# sudo rm -rf data
sudo rm -f jobs/nohup.out

cur_date="`date +%Y-%m-%d`"
DOCKER_REPO="dxglaw/pythonstock"
DOCKER_TAG=${DOCKER_REPO}:latest
DOCKER_TAG2=${DOCKER_REPO}:${cur_date}
# echo ${DOCKER_TAG}
# echo ${DOCKER_TAG2}

echo " docker build -f Dockerfile -t ${DOCKER_TAG} -t ${DOCKER_TAG2} ."
docker build -f Dockerfile -t ${DOCKER_TAG} -t ${DOCKER_TAG2} .
echo "#################################################################"
echo " docker push ${DOCKER_TAG} ${DOCKER_TAG2}"
docker push ${DOCKER_TAG}
docker push ${DOCKER_TAG2}

# mkdir data

