#!/bin/sh
bsub -Is -R "span[ptile=4]" -M 48000 -R "rusage[mem=12000]" -n 16 /homes/federico/impc-etl/bin/lsf-spark-submit.sh "$@"
