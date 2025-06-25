import warnings
from typing import Iterable, Literal
import re
import pandas as pd

REPLACE_WITH_SPACES = r"[!\"$\%'()\+,\-./:;?]"
    
def prepare_text(
    text: str,
    case_sensitive: bool,
    spaces_sensitive: bool,
    replace_with_spaces=REPLACE_WITH_SPACES,
) -> str:
    """Normalize text for keywords search"""
    if not case_sensitive:
        text = text.lower()
    if not spaces_sensitive:
        text = text.replace(" ", "")
    else:

        text = re.sub(replace_with_spaces, " ", text)
        text = " " + text.strip() + " "
    return text
    
class _MappingDict:
    """
    Internal helper class used by MappingRules to store normalized keyword mappings.
    This class is not intended for direct use.

    Attributes:
        rules (dict): A mapping of keywords to values.
        case_sensitive (bool): Whether keyword search is case-sensitive.
        spaces_sensitive (bool): Whether keyword search should preserve spaces and special characters.
        is_prepared (bool): Whether rules are already normalized.

    Methods:
        prepare():
            Normalizes all keywords in the mapping according to the case and space sensitivity rules.
    """

    def __init__(
        self,
        rules: dict,
        case_sensitive: bool = False,
        spaces_sensitive: bool = False,
    ):
        self.rules = rules
        self.case_sensitive = case_sensitive
        self.spaces_sensitive = spaces_sensitive
        self.is_prepared = False

    def prepare(self):
        if not self.is_prepared:
            prepared_rules = {}
            for key, val in self.rules.items():
                prepared_key = prepare_text(
                    key, self.case_sensitive, self.spaces_sensitive
                )
                prepared_rules[prepared_key] = val
                for char in REPLACE_WITH_SPACES:
                    if self.spaces_sensitive and prepared_key != key and char in key:
                        warnings.warn(
                            f"'{char}' in the keyword '{key}' was replaced with a space.",
                            UserWarning,
                        )
            self.rules = prepared_rules
            self.is_prepared = True
        else:
            warnings.warn("MappingDict is already prepared.", UserWarning)
        return self


class MappingRules:
    """
    Handles keyword-based mapping rules with configurable text normalization.

    This class is responsible for validating, preparing, and applying mapping rules
    defined in a pandas DataFrame. Each rule maps a keyword to a result value,
    optionally specifying case and space sensitivity. The rules are grouped
    by normalization settings and transformed into MappingDicts for fast lookup.

    Attributes:
        attr_name (str): The name of the attribute the rules apply to (used for error messages).
        rules_df (pd.DataFrame): DataFrame containing the rules, must have columns:
            ["keyword", "result", "case_sensitive", "spaces_sensitive"].
        _is_prepared (bool): Indicates whether the rules have been processed into MappingDicts.

    Methods:
        prepare():
            Groups and transforms the rules into MappingDicts according to their normalization settings.
        apply(texts, find):
            Applies the mapping to a list of texts, returning either the first match ("any")
            or a set of all matches ("all").
    """

    def __init__(self, attr_name: str, rules_df: pd.DataFrame):
        self.attr_name = attr_name
        self.rules_df = rules_df[
            ["keyword", "result", "case_sensitive", "spaces_sensitive"]
        ]

        # "keyword" is allowed to be null if "keyword" is meant to be the same as "result" 
        self._check_for_nulls()
        self.rules_df["keyword"] = self.rules_df.apply(
            lambda x: x.result if pd.isna(x.keyword) else x.keyword, axis="columns"
        )
        
        self._check_keywords_uniqueness()
        self._is_prepared = False

    def _check_for_nulls(self):
        for col in ["result", "case_sensitive", "spaces_sensitive"]:
            if self.rules_df[col].isna().sum() > 0:
                raise ValueError(
                    f"'{col}' can not be Null. Fix mapping rules for {self.attr_name}."
                )

    def _check_keywords_uniqueness(self):
        non_unique_keywords = self.rules_df["keyword"].value_counts()[lambda x: x > 1]
        if len(non_unique_keywords) > 0:
            non_unique_keywords_str = ", ".join(non_unique_keywords.index)
            raise ValueError(
                f"Keywords must be unique. Fix mapping rules for {self.attr_name}: {non_unique_keywords_str}."
            )

    def prepare(self):
        if not self._is_prepared:
            self.map_dicts = list(
                self.rules_df.set_index("keyword")
                .groupby(by=["case_sensitive", "spaces_sensitive"])
                .agg(dict)
                .rename({"result": "mapping_dict"}, axis="columns")
                .reset_index()
                .apply(
                    lambda x: _MappingDict(
                        rules=x.mapping_dict,
                        case_sensitive=x.case_sensitive,
                        spaces_sensitive=x.spaces_sensitive,
                    ),
                    axis="columns",
                )
            )
            self.map_dicts_prepared = [
                mapping_dict.prepare() for mapping_dict in self.map_dicts
            ]
            self._is_prepared = True
        else:
            warnings.warn("MappingRules are already prepared.", UserWarning)

    def apply(
        self, texts: Iterable[str], find: Literal["any", "all"] = "all"
    ) -> str | set[str] | None:

        if not self._is_prepared:
            self.prepare()

        if find == "all":
            result = set()
        if find == "any":
            result = None

        for mapping_dict in self.map_dicts_prepared:
            for text in texts:
                text = prepare_text(
                    text, mapping_dict.case_sensitive, mapping_dict.spaces_sensitive
                )
                for key, val in mapping_dict.rules.items():
                    if key in text:
                        if find == "all":
                            result.add(val)
                        if find == "any":
                            return val
        return result