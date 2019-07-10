from impc_etl.workflow.extraction import *
from impc_etl.workflow.cleaning import *


class ImpcEtl(luigi.Task):
    dcc_xml_path = luigi.Parameter()
    imits_allele2_tsv_path = luigi.Parameter()
    imits_report_tsv_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            ExperimentCleaner(xml_path=self.dcc_xml_path, output_path=self.output_path),
            LineCleaner(xml_path=self.dcc_xml_path, output_path=self.output_path),
            MouseCleaner(xml_path=self.dcc_xml_path, output_path=self.output_path),
            EmbryoCleaner(xml_path=self.dcc_xml_path, output_path=self.output_path),
            AlleleExtractor(tsv_path=self.imits_allele2_tsv_path, output_path=self.output_path),
            ColonyExtractor(tsv_path=self.imits_report_tsv_path, output_path=self.output_path),
            ImpressExtractor(output_path=self.output_path)
        ]
