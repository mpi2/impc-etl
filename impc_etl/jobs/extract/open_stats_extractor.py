import json
import re
import sys
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
    """
    jdbc_connection_str = argv[1]
    db_user = argv[2]
    db_password = argv[3]
    db_table = argv[4]
    use_cache = argv[5]
    raw_data_in_output = argv[6] == "include"
    extract_windowed_data = argv[7] == "true"
    output_path = argv[8]

    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
    }

    spark = SparkSession.builder.getOrCreate()
    if use_cache != "true":
        stats_df = spark.read.jdbc(
            jdbc_connection_str,
            table=f"(SELECT CAST(id AS BIGINT) AS numericId, * FROM {db_table}) AS tmp",
            properties=properties,
            numPartitions=5000,
            column="numericId",
            lowerBound=0,
            upperBound=999991716,
        )
        stats_df = stats_df.withColumnRenamed("statpacket", "json")
        stats_df.write.mode("overwrite").parquet(output_path + "_temp")
    stats_df = spark.read.parquet(output_path + "_temp").repartition(10000)
    if extract_windowed_data:
        stats_df = stats_df.where(col("json").contains("Windowed result"))

    dumping_function = dump_json
    if raw_data_in_output:
        dumping_function = dump_json_raw_data
    if extract_windowed_data:
        dumping_function = dump_json_windowed_data
    stats_df.rdd.map(dumping_function).saveAsTextFile(output_path + "_json_temp")
    json_df = spark.read.json(output_path + "_json_temp", mode="FAILFAST")
    json_df.write.mode("overwrite").parquet(output_path)


def partition_to_json(list_of_rows):
    json_str_list = []
    for row in list_of_rows:
        json_str_list.append(
            json.dumps(json.loads(row.json, object_pairs_hook=object_pairs_hook))
        )
    return f"[{','.join(json_str_list)}]"


def object_pairs_hook(lit):
    return dict(
        [
            (
                re.sub(
                    r"\{|\}|\(|\)|\\", "|", re.sub(r"\.|\s|,|;|\n|\||\t|=", "_", key)
                ),
                value
                if "Original" not in key
                and "othercolumns" not in key
                and "OpenStatsList" not in key
                else None,
            )
            for (key, value) in lit
            if re
        ]
    )


def dump_json_windowed_data(row):
    stats_result = _get_stat_result(row, True, True)
    json_str = json.dumps(stats_result)
    return json_str


def dump_json_raw_data(row):
    stats_result = _get_stat_result(row, True)
    json_str = json.dumps(stats_result)
    return json_str


def dump_json(row):
    stats_result = _get_stat_result(row)
    json_str = json.dumps(stats_result)
    return json_str


def _get_stat_result(row, include_raw_data=False, extract_windowed_data=False):
    packet_result_map = {
        "gene_accession_id": "marker_accession_id",
        "gene_symbol": "marker_symbol",
    }
    stats_packet = json.loads(row.json)
    stats_packet_detail = stats_packet["Result"]["Details"]
    experiment_detail = stats_packet_detail["Experiment detail"]
    stats_result = {
        key if key not in packet_result_map else packet_result_map[key]: value
        for key, value in experiment_detail.items()
    }
    stats_result["data_type"] = stats_packet_detail["Observation type"]
    stats_result.update(get_raw_data_details(stats_packet_detail))

    if stats_result["status"] == "Successful":
        try:
            if extract_windowed_data:
                normal_result = stats_packet["Result"]["Vector output"][
                    "Windowed result"
                ]
            else:
                normal_result = stats_packet["Result"]["Vector output"]["Normal result"]
            stats_result["statistical_method"] = (
                normal_result["Applied method"]
                if "Applied method" in normal_result
                else stats_packet_detail["Applied method"]
            )
        except Exception as e:
            raise type(e)(str(e) + " happens at %s" % str(stats_packet)).with_traceback(
                sys.exc_info()[2]
            )
        stats_result.update(
            get_calculation_details(normal_result, stats_result["statistical_method"])
        )
        stats_result["mp_term"] = (
            stats_packet_detail["MPTERM"] if "MPTERM" in stats_packet_detail else None
        )
    if include_raw_data:
        stats_result["observations_biological_sample_group"] = _process_raw_data_field(
            "Original_biological_sample_group", stats_packet_detail
        )
        stats_result["observations_body_weight"] = _process_raw_data_field(
            "Original_body_weight", stats_packet_detail
        )
        stats_result["observations_date_of_experiment"] = _process_raw_data_field(
            "Original_date_of_experiment", stats_packet_detail
        )
        stats_result["observations_external_sample_id"] = _process_raw_data_field(
            "Original_external_sample_id", stats_packet_detail
        )
        stats_result["observations_response"] = _process_raw_data_field(
            "Original_response", stats_packet_detail
        )
        stats_result["observations_time_point"] = _process_raw_data_field(
            "Original_time_point", stats_packet_detail
        )
        stats_result["observations_discrete_point"] = _process_raw_data_field(
            "Original_discrete_point", stats_packet_detail
        )
        stats_result["observations_sex"] = _process_raw_data_field(
            "Original_sex", stats_packet_detail
        )
        stats_result["observations_id"] = _process_raw_data_field(
            "Original_observation_id", stats_packet_detail
        )

        if extract_windowed_data:
            window_parameters = stats_packet_detail["Window parameters"]
            stats_result["window_l_value"] = window_parameters["l"]["value"]
            stats_result["window_l_score"] = window_parameters["l"]["score"]
            stats_result["window_k_value"] = window_parameters["k"]["value"]
            stats_result["window_k_score"] = window_parameters["k"]["score"]
            stats_result["window_doe"] = window_parameters["DOE"]
            stats_result["window_min_obs_required"] = window_parameters[
                "Min obs required in the window"
            ]
            stats_result["window_total_obs_or_weight"] = (
                window_parameters["Total obs or weight in the window"]
                if "Total obs or weight in the window" in window_parameters
                else None
            )
            stats_result["window_threshold"] = (
                window_parameters["Threshold"]
                if "Threshold" in window_parameters
                else None
            )
            stats_result["window_number_of_doe"] = (
                window_parameters["The number of DOE in the window"]
                if "The number of DOE in the window" in window_parameters
                else None
            )
            stats_result["window_doe_note"] = (
                window_parameters["DOE note"]
                if "DOE note" in window_parameters
                else None
            )
            weight_dictionary = dict(
                zip(
                    window_parameters["external_sample_id"],
                    window_parameters["Window weights"],
                )
            )
            stats_result["observations_window_weight"] = []
            for sample_id in stats_result["observations_external_sample_id"]:
                if sample_id in weight_dictionary:
                    stats_result["observations_window_weight"].append(
                        weight_dictionary[sample_id]
                    )
                else:
                    stats_result["observations_window_weight"].append(None)
    return stats_result


def get_raw_data_details(stats_packet_detail) -> Dict:
    try:
        packet_result_map = {
            "male_control": "male_control",
            "female_control": "female_control",
            "male_experimental": "male_mutant",
            "female_experimental": "female_mutant",
            "no data_control": "no_data_control",
            "no data_experimental": "no_data_mutant",
            "both_control": "both_control",
            "both_experimental": "both_mutant",
        }
        raw_data_details = {}
        if "Raw data summary statistics" not in stats_packet_detail:
            return {}
        stats_packet_raw_summary: Dict = stats_packet_detail[
            "Raw data summary statistics"
        ]["Collapsed"]
        for sample_group, collapsed_stats in stats_packet_raw_summary.items():
            if sample_group in packet_result_map:
                mapped_sample_group = packet_result_map[sample_group]
                for stats in collapsed_stats.values():
                    for stat, value in stats.items():
                        if type(value) == dict:
                            continue
                        stat = re.sub(
                            r"\{|\}|\(|\)|\\|\"",
                            "|",
                            re.sub(r"\.|\s|,|;|\n|\||\t|=", "_", stat),
                        ).lower()
                        if stat == "count":
                            stat = f"{mapped_sample_group}_{stat.lower()}"
                            if stat not in raw_data_details:
                                raw_data_details[stat] = value
                            else:
                                raw_data_details[stat] += value
                        else:
                            raw_data_details[
                                f"{mapped_sample_group}_{stat.lower()}"
                            ] = value
        return raw_data_details
    except Exception as e:
        raise type(e)(
            str(e) + " happens at %s" % str(stats_packet_detail)
        ).with_traceback(sys.exc_info()[2])


def _process_raw_data_field(original_name, stats_packet_detail):
    if original_name in stats_packet_detail:
        if type(stats_packet_detail[original_name]) == list:
            return stats_packet_detail[original_name]
        else:
            return [stats_packet_detail[original_name]]
    else:
        return []


def get_calculation_details(normal_result, applied_method):
    try:
        calculation_details = {"additional_information": None}

        mm_regex = re.compile(r".*Mixed Model.*")
        lm_regex = re.compile(r".*Linear Model.*")
        if (
            mm_regex.search(applied_method)
            or lm_regex.search(applied_method)
            or applied_method == "MM"
        ):
            calculation_details["p_value"] = (
                normal_result["Genotype p-value"]
                if "Genotype p-value" in normal_result
                else None
            )
            calculation_details["effect_size"] = (
                normal_result["Genotype effect size"]["Value"]
                if "Genotype effect size" in normal_result
                else None
            )

            calculation_details["male_effect_size"] = (
                normal_result["Sex MvKO effect size"]["Value"]
                if "Sex MvKO effect size" in normal_result
                else None
            )

            calculation_details["female_effect_size"] = (
                normal_result["Sex FvKO effect size"]["Value"]
                if "Sex FvKO effect size" in normal_result
                else None
            )

            calculation_details["batch_significant"] = (
                normal_result["Batch included"]
                if "Batch included" in normal_result
                else None
            )
            calculation_details["variance_significant"] = (
                normal_result["Residual variances homogeneity"]
                if "Residual variances homogeneity" in normal_result
                else None
            )
            calculation_details["genotype_effect_p_value"] = (
                normal_result["Genotype p-value"]
                if "Genotype p-value" in normal_result
                else None
            )
            calculation_details["genotype_effect_stderr_estimate"] = (
                normal_result["Genotype standard error"]
                if "Genotype standard error" in normal_result
                else None
            )
            calculation_details["genotype_effect_parameter_estimate"] = (
                normal_result["Genotype estimate"]["Value"]
                if "Genotype estimate" in normal_result
                else None
            )

            if "Genotype percentage change" in normal_result:
                genotype_percentage_change = normal_result["Genotype percentage change"]

                calculation_details["male_percentage_change"] = (
                    genotype_percentage_change["SexMale:Genotypeexperimental"]
                    if "SexMale:Genotypeexperimental" in genotype_percentage_change
                    else None
                )
                calculation_details["female_percentage_change"] = (
                    genotype_percentage_change["SexFemale:Genotypeexperimental"]
                    if "SexFemale:Genotypeexperimental" in genotype_percentage_change
                    else None
                )
                calculation_details["percentage_change"] = (
                    genotype_percentage_change["experimental Genotype"]
                    if "experimental Genotype" in genotype_percentage_change
                    else None
                )
            calculation_details["sex_effect_p_value"] = (
                normal_result["Sex p-value"] if "Sex p-value" in normal_result else None
            )
            calculation_details["sex_effect_stderr_estimate"] = (
                normal_result["Sex standard error"]
                if "Sex standard error" in normal_result
                else None
            )
            calculation_details["sex_effect_parameter_estimate"] = (
                normal_result["Sex estimate"]["Value"]
                if "Sex estimate" in normal_result
                else None
            )

            calculation_details["group_1_genotype"] = (
                normal_result["Gp1 genotype"]
                if "Gp1 genotype" in normal_result
                else None
            )
            calculation_details["group_1_residuals_normality_test"] = (
                normal_result["Gp1 Residuals normality test"]["P-value"]
                if "Gp1 Residuals normality test" in normal_result
                and "P-value" in normal_result["Gp1 Residuals normality test"]
                else None
            )
            calculation_details["group_2_genotype"] = (
                normal_result["Gp2 genotype"]
                if "Gp2 genotype" in normal_result
                else None
            )
            calculation_details["group_2_residuals_normality_test"] = (
                normal_result["Gp2 Residuals normality test"]["P-value"]
                if "Gp2 Residuals normality test" in normal_result
                and "P-value" in normal_result["Gp2 Residuals normality test"]
                else None
            )

            calculation_details["intercept_estimate"] = (
                normal_result["Intercept estimate"]["Value"]
                if "Intercept estimate" in normal_result
                else None
            )
            calculation_details["intercept_estimate_stderr_estimate"] = (
                normal_result["Intercept standard error"]
                if "Intercept standard error" in normal_result
                else None
            )

            calculation_details["interaction_significant"] = (
                normal_result["Interactions included"]["Genotype Sex"]
                if "Interactions included" in normal_result
                else None
            )
            calculation_details["interaction_effect_p_value"] = (
                normal_result["Interactions p-value"]["Genotype Sex"]
                if "Interactions p-value" in normal_result
                else None
            )

            calculation_details["female_ko_effect_p_value"] = (
                normal_result["Sex FvKO p-value"]
                if "Sex FvKO p-value" in normal_result
                else None
            )
            calculation_details["female_ko_effect_stderr_estimate"] = (
                normal_result["Sex FvKO standard error"]
                if "Sex FvKO standard error" in normal_result
                else None
            )
            calculation_details["female_ko_parameter_estimate"] = (
                normal_result["Sex FvKO estimate"]["Value"]
                if "Sex FvKO estimate" in normal_result
                else None
            )

            calculation_details["male_ko_effect_p_value"] = (
                normal_result["Sex MvKO p-value"]
                if "Sex MvKO p-value" in normal_result
                else None
            )
            calculation_details["male_ko_effect_stderr_estimate"] = (
                normal_result["Sex MvKO standard error"]
                if "Sex MvKO standard error" in normal_result
                else None
            )
            calculation_details["male_ko_parameter_estimate"] = (
                normal_result["Sex MvKO estimate"]["Value"]
                if "Sex MvKO estimate" in normal_result
                else None
            )

        rr_regex = re.compile(r".*Reference Range.*")
        if rr_regex.search(applied_method):
            genotype_p_value = normal_result["Genotype p-value"]
            calculation_details["genotype_pvalue_low_vs_normal_high"] = (
                genotype_p_value["Low"]["p.value"]
                if "Low" in genotype_p_value
                else None
            )
            calculation_details["genotype_pvalue_low_normal_vs_high"] = (
                genotype_p_value["High"]["p.value"]
                if "High" in genotype_p_value
                else None
            )

            genotype_effect_size = normal_result["Genotype effect size"]
            calculation_details["genotype_effect_size_low_vs_normal_high"] = (
                genotype_effect_size["Low"]["effect"]["value"]
                if "Low" in genotype_effect_size
                else None
            )
            genotype_effect_size = normal_result["Genotype effect size"]
            calculation_details["genotype_effect_size_low_normal_vs_high"] = (
                genotype_effect_size["High"]["effect"]["value"]
                if "High" in genotype_effect_size
                else None
            )

            if "Sex FvKO p-value" in normal_result:
                female_vs_ko_p_value = normal_result["Sex FvKO p-value"]
                calculation_details["female_pvalue_low_vs_normal_high"] = (
                    female_vs_ko_p_value["Low"]["p.value"]
                    if "Low" in female_vs_ko_p_value
                    else None
                )
                calculation_details["female_pvalue_low_normal_vs_high"] = (
                    female_vs_ko_p_value["High"]["p.value"]
                    if "High" in female_vs_ko_p_value
                    else None
                )

                female_vs_ko_effect = normal_result["Sex FvKO effect size"]
                calculation_details["female_pvalue_low_vs_normal_high"] = (
                    female_vs_ko_effect["Low"]["effect"]["value"]
                    if "Low" in female_vs_ko_effect
                    else None
                )
                calculation_details["female_pvalue_low_normal_vs_high"] = (
                    female_vs_ko_effect["High"]["effect"]["value"]
                    if "High" in female_vs_ko_effect
                    else None
                )

            if "Sex MvKO p-value" in normal_result:
                male_vs_ko_p_value = normal_result["Sex MvKO p-value"]
                calculation_details["male_pvalue_low_vs_normal_high"] = (
                    male_vs_ko_p_value["Low"]["p.value"]
                    if "Low" in male_vs_ko_p_value
                    else None
                )
                calculation_details["male_pvalue_low_normal_vs_high"] = (
                    male_vs_ko_p_value["High"]["p.value"]
                    if "High" in male_vs_ko_p_value
                    else None
                )
            if "Sex MvKO effect size" in normal_result:
                male_vs_ko_effect = normal_result["Sex MvKO effect size"]
                calculation_details["male_effect_size_low_vs_normal_high"] = (
                    male_vs_ko_effect["Low"]["effect"]["value"]
                    if "Low" in male_vs_ko_effect
                    else None
                )
                calculation_details["male_effect_size_low_normal_vs_high"] = (
                    male_vs_ko_effect["High"]["effect"]["value"]
                    if "High" in male_vs_ko_effect
                    else None
                )
            if "Sex FvKO effect size" in normal_result:
                female_vs_ko_effect = normal_result["Sex FvKO effect size"]
                calculation_details["female_effect_size_low_vs_normal_high"] = (
                    female_vs_ko_effect["Low"]["effect"]["value"]
                    if "Low" in female_vs_ko_effect
                    else None
                )
                calculation_details["female_effect_size_low_normal_vs_high"] = (
                    female_vs_ko_effect["High"]["effect"]["value"]
                    if "High" in female_vs_ko_effect
                    else None
                )
        fisher_regex = re.compile(r".*Fisher Exact.*")
        if fisher_regex.search(applied_method):
            calculation_details["p_value"] = (
                normal_result["Genotype p-value"]["Complete table"]["p.value"]
                if "Genotype p-value" in normal_result
                else None
            )
            calculation_details["effect_size"] = (
                normal_result["Genotype effect size"]["Complete table"]["effect"][
                    "value"
                ]
                if "Genotype effect size" in normal_result
                else None
            )
            calculation_details["interaction_significant"] = (
                normal_result["Interactions included"]["Genotype Sex"]
                if "Interactions included" in normal_result
                else None
            )
            calculation_details["interaction_effect_p_value"] = (
                normal_result["Interactions p-value"]["Genotype Sex"]
                if "Interactions p-value" in normal_result
                else None
            )
            calculation_details["female_ko_effect_p_value"] = (
                normal_result["Sex FvKO p-value"]["Complete table"]["p.value"]
                if "Sex FvKO p-value" in normal_result
                and "Complete table" in normal_result["Sex FvKO p-value"]
                else None
            )
            calculation_details["female_ko_parameter_estimate"] = (
                normal_result["Sex FvKO estimate"]["Complete table"]["Value"]
                if "Sex FvKO estimate" in normal_result
                else None
            )
            calculation_details["male_ko_effect_p_value"] = (
                normal_result["Sex MvKO p-value"]["Complete table"]["p.value"]
                if "Sex MvKO p-value" in normal_result
                and "Complete table" in normal_result["Sex MvKO p-value"]
                else None
            )
            calculation_details["male_ko_parameter_estimate"] = (
                normal_result["Sex MvKO estimate"]["Complete table"]["Value"]
                if "Sex MvKO estimate" in normal_result
                else None
            )
        calculation_details["classification_tag"] = (
            normal_result["Classification tag"]["Classification tag"]
            if "Classification tag" in normal_result
            and "Classification tag" in normal_result["Classification tag"]
            else None
        )
        calculation_details["phenotype_sex"] = (
            normal_result["Additional information"]["Analysis"][
                "Gender included in analysis"
            ]
            if "Additional information" in normal_result
            and "Analysis" in normal_result["Additional information"]
            else None
        )
        calculation_details["weight_effect_p_value"] = (
            normal_result["Weight p-value"]
            if "Weight p-value" in normal_result
            else None
        )
        calculation_details["weight_effect_stderr_estimate"] = (
            normal_result["Weight standard error"]
            if "Weight standard error" in normal_result
            else None
        )
        calculation_details["weight_effect_parameter_estimate"] = (
            normal_result["Weight estimate"]["Value"]
            if "Weight estimate" in normal_result
            and "Value" in normal_result["Weight estimate"]
            else None
        )
        return calculation_details
    except KeyError as e:
        raise type(e)(str(e) + " happens at %s" % str(normal_result)).with_traceback(
            sys.exc_info()[2]
        )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
