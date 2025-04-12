from collections import namedtuple

MappingDict = namedtuple(
    "MappingDict", ["mapping_dict", "case_sensitive", "spaces_sensitive"]
)

city_clusters = {
    "Rhein-Neckar": {"mannheim", "heidelberg", "walldorf", "ludwigshafen"},
    "Berlin": {"berlin"},
    "Munich": {"munich", "münchen", "munchen", "münchen"},
    "Other": {"oder"},  # catch another Frankfurt!
    "Frankfurt": {"frankfurt"},
    "Cologne": {"cologne", "köln"},
    "Düsseldorf": {"düsseldorf", "duesseldorf"},
    "Stuttgart": {"stuttgart"},
    "Hamburg": {"hamburg"},
    "Hanover": {"hannover", "hanover"},
    "Nuremberg": {"nürnberg", "nuremberg"},
    "Bonn": {"bonn"},
    "Leipzig": {"leipzig"},
    "Dresden": {"dresden"},
    "Essen": {"essen"},
    "Karlsruhe": {"karlsruhe"},
    "Bremen": {"bremen"},
    "Potsdam": {"potsdam"},
    "Heidelberg": {"heidelberg"},
}

mapping_dicts_positions = [
    MappingDict(
        {
            "Data Engineer": "Data Engineer",
            "Data Anal": "Data Analyst",
            "Data Scien": "Data Scientist",
            "Data Manager": "Data Manager",
            "Product Manager": "Product Manager",
            "Project Manager": "Project Manager",
            "Product Owner": "Product Owner",
            "ML Ops": "ML Ops",
            "Platform Engineer": "IaC Specialist",
            "Software Engineer": "Software Engineer",
            "Network Engineer": "Network Engineer",
            "Facility Engineer": "Facility Engineer",
            "Governence": "Data Protection/Governance Specialist",
            "Governance": "Data Protection/Governance Specialist",
            "Data Privacy": "Data Protection/Governance Specialist",
            "Data Protection": "Data Protection/Governance Specialist",
            "Data Entry": "Data Entry Specialist",
            "Data Quality": "Data Quality Specialist",
            "BI Engineer": "Data Engineer",
            "BI Anal": "Data Analyst",
            "Marketing Anal": "Data Analyst",
            "Machine Learning Engineer": "ML Ops",
            "Data Steward": "Data Manager",
        },
        False,
        False,
    ),
    MappingDict(
        {
            "AI Engineer": "ML Ops",
            "Data Research": "Data Analyst",
            "Data Modelling": "Data Manager",
            "Data Mapping": "Data Manager",
            "Validation": "Data Quality Specialist",
            "Analyst": "Data Analyst",
        },
        False,
        False,
    ),
    MappingDict(
        {
            "Infrastructure Engineer": "IaC Specialist",
            "Architect": "Architect",
            "Data Center Technician": "Facility Engineer",
            "DevOps": "IaC Specialist",
            "Tutor": "Tutor/Teacher",
            "Teacher": "Tutor/Teacher",
            "Consultant": "Consultant/Advisor",
            "Advisor": "Consultant/Advisor",
        },
        False,
        False,
    ),
    MappingDict(
        {
            "DE": "Data Engineer",
            "IaC": "IaC Specialist",
            "DA": "Data Analyst",
            "DS": "Data Scientist",
        },
        True,
        True,
    ),
]


def prepare_mapping_dict(
    mapping_dict: dict, case_sensitive: bool = False, spaces_sensitive: bool = False
) -> MappingDict:
    prepared_dict = {}
    for key, val in mapping_dict.items():
        prepared_key = key
        prepared_val = val
        if not case_sensitive:
            prepared_key = prepared_key.lower()
        if not spaces_sensitive:
            prepared_key = prepared_key.replace(" ", "")
        prepared_dict[prepared_key] = prepared_val
    return MappingDict(
        prepared_dict,
        case_sensitive,
        spaces_sensitive,
    )

