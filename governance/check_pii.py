"""Module 5: fail CI if a pii-tagged column reaches a mart without a masking macro.

TODO:
- load warehouse/dbt/target/manifest.json
- for each column with meta.pii == true, follow lineage to marts
- fail unless the mart column's compiled SQL uses a macro from pii_policy.yml
"""
