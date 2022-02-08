#!/bin/bash --login
set -e

conda activate $HOME/load_bases/env
exec "$@"