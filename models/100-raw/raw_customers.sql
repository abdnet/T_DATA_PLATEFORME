{{ config(
    tags=["PIPELINE_CUSTOMER"]
) }}

{{ mirror_raw("PUBLIC","CUSTOMERS") }}
