from impc_etl.workflow.normalization import *


class ImpcEtl(luigi.Task):
    dcc_xml_path = luigi.Parameter()
    imits_allele2_tsv_path = luigi.Parameter()
    imits_report_tsv_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            ExperimentNormalizer(xml_path=self.dcc_xml_path, tsv_path=self.imits_report_tsv_path,
                                 entity_type='experiment', output_path=self.output_path),
            LineCleaner(xml_path=self.dcc_xml_path, output_path=self.output_path),
            AlleleExtractor(tsv_path=self.imits_allele2_tsv_path, output_path=self.output_path),
        ]
