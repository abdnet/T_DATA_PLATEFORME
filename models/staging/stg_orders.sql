with
    source as (select * from {{ source("SOURCE_API", "SRC_ORDERS") }}),

    renamed as (select * from source),

    final as (select * from renamed)

select *
from final
