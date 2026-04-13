import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.attribute_flags import is_truthy, supports_business_or_bike_parking
from utils.date_year_parser import extract_year
from utils.key_normalization import normalize_lower, yelp_business_id_to_ref


def test_yelp_business_id_to_ref_maps_prefix():
    assert yelp_business_id_to_ref("businessid_52") == "businessref_52"


def test_normalize_lower_trims_and_lowercases():
    assert normalize_lower("  New York ") == "new york"


def test_extract_year_handles_mixed_date_text():
    assert extract_year("March 5, 2018 at 8:41 PM") == 2018
    assert extract_year("2016-06-30") == 2016
    assert extract_year("unknown") is None


def test_truthy_and_parking_parsing():
    assert is_truthy("True") is True
    assert is_truthy("u'no'") is False
    attrs = {"BikeParking": False, "BusinessParking": "{'garage': False, 'street': True}"}
    assert supports_business_or_bike_parking(attrs) is True
