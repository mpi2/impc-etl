from impc_etl.jobs.clean.colony_cleaner import *


class TestColonyCleaner:
    """
    Applying map_strain_name to string should return a mapped strain name
    After generate_genetic_background should add a new column with the computed genetic background
    """

    def test_map_strain_name(self):
        assert map_strain_name(None) is None
        assert map_strain_name("STRAIN1_STRAIN2") == "STRAIN1 * STRAIN2"
        assert map_strain_name("STRAIN1;STRAIN2") == "STRAIN1 * STRAIN2"
        assert map_strain_name("Balb/c.129S2") == "BALB/c * 129S2/SvPas"

        for strain_name in ["B6N.129S2.B6J", "B6J.129S2.B6N", "B6N.B6J.129S2"]:
            assert map_strain_name(strain_name) == "C57BL/6N * 129S2/SvPas * C57BL/6J"

        assert map_strain_name("B6J.B6N") == "C57BL/6J * C57BL/6N"
        assert map_strain_name("OTHERSTRAIN") == "OTHERSTRAIN"
