#!/bin/bash --login
set -e
export LOCAL_IP=$(getent hosts ${HOSTNAME} | awk '{print $1}')
conda activate $HOME/conda_jupyter/system/env
exec "$@"