# Utils Directory

Shared reusable modules used by Oracle Forge.

## Modules

1. `utils/key_normalization.py`
- What it does: Normalizes and maps cross-source IDs (for example Yelp `businessid_*` to `businessref_*`).
- Where used: cross-source join preparation and benchmark diagnostics.
- Usage example:

```python
from utils.key_normalization import yelp_business_id_to_ref
assert yelp_business_id_to_ref("businessid_52") == "businessref_52"
```

2. `utils/date_year_parser.py`
- What it does: Extracts a four-digit year from mixed-format date text.
- Where used: query logic that must be robust to inconsistent date formatting.
- Usage example:

```python
from utils.date_year_parser import extract_year
assert extract_year("March 5, 2018 at 8:41 PM") == 2018
```

3. `utils/attribute_flags.py`
- What it does: Parses truthy/falsey metadata flags and parking support attributes.
- Where used: Yelp-style attribute filtering such as bike/business parking logic.
- Usage example:

```python
from utils.attribute_flags import supports_business_or_bike_parking
attrs = {"BikeParking": "True", "BusinessParking": "{'garage': False, 'street': True}"}
assert supports_business_or_bike_parking(attrs) is True
```

## Tests

Utility tests are in `tests/test_utils_modules.py`.

Run:

```bash
python -m pytest tests/test_utils_modules.py -q
```

## Notes

Runtime modules under `src/` may retain local helper implementations, but utility-grade logic should converge here for reuse and testing.
