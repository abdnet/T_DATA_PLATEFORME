{{ config(
    tags=["PIPELINE_PAYMENTS"]
) }}

{{ mirror_raw("PUBLIC","PAYMENTS") }}
