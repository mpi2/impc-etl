import os.path
import sys

STATS_DATA_EMAIL_FILE = 'stats_data.email.txt'
DIFF_REPORT_EMAIL_FILE = 'diff_report.email.txt'

OBS_PARQUET_LOCATION = '<OBS_PARQUET_LOCATION>'
MP_CHOOSER_LOCATION = '<MP_CHOOSER_LOCATION>'
CURRENT_DR = '<CURRENT_DR>'
PREV_DR = '<PREV_DR>'


def main(drTag, prevDr, emailsFolder, etlDir):
    with open(os.path.join(emailsFolder, STATS_DATA_EMAIL_FILE), 'r') as fh:
        content = fh.read()
    newContent = content.replace(CURRENT_DR, drTag)
    newContent = newContent.replace(OBS_PARQUET_LOCATION,
                                    os.path.join(etlDir, drTag) + '/output/flatten_observations_parquet')

    mpChooserFileName = ''
    for file in os.listdir(emailsFolder):
        if file.startswith('part') and file.endswith('.txt'):
            mpChooserFileName = file
            break
    newContent = newContent.replace(MP_CHOOSER_LOCATION,
                                    os.path.join(etlDir, drTag) + '/output/mp_chooser_json/' + mpChooserFileName)

    with open(os.path.join(emailsFolder, STATS_DATA_EMAIL_FILE), 'w') as fh:
        fh.write(newContent)

    with open(os.path.join(emailsFolder, DIFF_REPORT_EMAIL_FILE), 'r') as fh:
        content = fh.read()
    newContent = content.replace(CURRENT_DR, drTag)
    newContent = newContent.replace(PREV_DR, prevDr)
    with open(os.path.join(emailsFolder, DIFF_REPORT_EMAIL_FILE), 'w') as fh:
        fh.write(newContent)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
