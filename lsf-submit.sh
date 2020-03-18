#!/bin/sh
bsub -Is -P bigmem -R "span[ptile=4]" -M 256000 -R "rusage[mem=256000]" -n 32 /homes/federico/impc-etl/bin/lsf-spark-submit.sh "$@"