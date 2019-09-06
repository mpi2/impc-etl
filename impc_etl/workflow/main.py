from impc_etl.workflow.load import *


class ImpcEtl(luigi.Task):
    dcc_xml_path = luigi.Parameter()
    imits_allele2_tsv_path = luigi.Parameter()
    imits_report_tsv_path = luigi.Parameter()
    strain_input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            StatsPipeLineLoader(
                xml_path=self.dcc_xml_path,
                imits_report_tsv_path=self.imits_report_tsv_path,
                imits_allele2_tsv_path=self.imits_allele2_tsv_path,
                output_path=self.output_path,
                strain_report_tsv_path=self.strain_input_path,
            )
        ]

    # def requires(self):
    #     return [ImpressExtractor(output_path=self.output_path)]
