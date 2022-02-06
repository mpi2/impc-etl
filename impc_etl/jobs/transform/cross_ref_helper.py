def generate_allelic_composition(
    zigosity: str,
    allele_symbol: str,
    gene_symbol: str,
    is_baseline: bool,
    colony_id: str,
):
    if is_baseline or colony_id == "baseline":
        return ""

    if zigosity in ["homozygous", "homozygote"]:
        if allele_symbol is not None and allele_symbol is not "baseline":
            if allele_symbol is not "" and " " not in allele_symbol:
                return f"{allele_symbol}/{allele_symbol}"
            else:
                return f"{gene_symbol}<?>/{gene_symbol}<?>"
        else:
            return f"{gene_symbol}<+>/{gene_symbol}<+>"

    if zigosity in ["heterozygous", "heterozygote"]:
        if allele_symbol is not "baseline":
            if (
                allele_symbol is not None
                and allele_symbol is not ""
                and " " not in allele_symbol
            ):
                return f"{allele_symbol}/{gene_symbol}<+>"
            else:
                return f"{gene_symbol}<?>/{gene_symbol}<+>"
        else:
            return None

    if zigosity in ["hemizygous", "hemizygote"]:
        if allele_symbol is not "baseline":
            if (
                allele_symbol is not None
                and allele_symbol is not ""
                and " " not in allele_symbol
            ):
                return f"{allele_symbol}/0"
            else:
                return f"{gene_symbol}<?>/0"
        else:
            return None
