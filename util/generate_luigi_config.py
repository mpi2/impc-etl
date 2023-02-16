import os
import sys

MANDATORY_ARGUMENTS = ['--template', '--output', '--drtag', '--prevDrTag']
ARG_STATS_OUTPUT = '--statsOutputPath'


def main(arguments):
    if not os.path.isfile(arguments['--template']):
        print('Template file provided [{}] does not exist!'.format(arguments['--template']))
        exit(-1)

    with open(arguments['--template'], 'r') as fh:
        content = fh.read()

    newContent = content.replace('<DR_TAG>', arguments['--drtag'])
    newContent = newContent.replace('<PREV_DR_TAG>', arguments['--prevDrTag'])
    if ARG_STATS_OUTPUT in arguments:
        newContent = newContent.replace('<STATS_DATA_LOCATION>', arguments[ARG_STATS_OUTPUT])

    with open(arguments['--output'], 'w') as fh:
        fh.write(newContent)


if __name__ == '__main__':
    if len(sys.argv) < 5:
        print(
            'Usage: generate_luigi_config.py --template=<template_file> --output=<output_file> --drtag=<DR_TAG> --prevDrTag=<PREV_DR_TAG> --statsOutputPath=<STATS_OUTPUT_PATH>')
        exit(-1)

    arguments = {}

    for i in range(1, len(sys.argv)):
        arg = sys.argv[i]
        argSplit = arg.split('=')
        arguments[argSplit[0]] = argSplit[1]

    errorFound = False
    for arg in MANDATORY_ARGUMENTS:
        if not arg in arguments:
            print(' -- Error: {} value is missing.'.format(arg))
            errorFound = True

    if errorFound:
        exit(-1)

    main(arguments)
