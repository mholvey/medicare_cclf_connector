select
      cast(CURRENT_CLAIM_UNIQUE_IDENTIFIER as {{ dbt.type_string() }} ) as claim_id
    , cast(CLAIM_LINE_NUMBER as integer) as claim_line_number
    , 'professional' as claim_type
    , cast(MEDICARE_BENEFICIARY_IDENTIFIER as {{ dbt.type_string() }} ) as patient_id
    , cast(MEDICARE_BENEFICIARY_IDENTIFIER as {{ dbt.type_string() }} ) as member_id
    , {{ try_to_cast_date('CLAIM_FROM_DATE', 'YYYY-MM-DD') }} as claim_start_date
    , {{ try_to_cast_date('CLAIM_THRU_DATE', 'YYYY-MM-DD') }} as claim_end_date
    , {{ try_to_cast_date('CLAIM_LINE_FROM_DATE', 'YYYY-MM-DD') }} as claim_line_start_date
    , {{ try_to_cast_date('CLAIM_LINE_THRU_DATE', 'YYYY-MM-DD') }} as claim_line_end_date
    , cast(NULL as date) as admission_date
    , cast(NULL as date) as discharge_date
    , cast(NULL as {{ dbt.type_string() }} ) as admit_source_code
    , cast(NULL as {{ dbt.type_string() }} ) as admit_type_code
    , cast(NULL as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(CLAIM_PLACE_OF_SERVICE_CODE as {{ dbt.type_string() }} ) as place_of_service_code
    , cast(NULL as {{ dbt.type_string() }} ) as bill_type_code
    , cast(NULL as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(NULL as {{ dbt.type_string() }} ) as apr_drg_code
    , cast(NULL as {{ dbt.type_string() }} ) as revenue_center_code
    , cast(NULL as integer) as service_unit_quantity
    , cast(HCPCS_CODE as {{ dbt.type_string() }} ) as hcpcs_code
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_1
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_2
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_3
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_4
    {# , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_5 #}
    , cast(CLAIM_ORDERING_PROVIDER_NPI_NUMBER as {{ dbt.type_string() }} ) as rendering_npi
    , cast(CLAIM_PAY_TO_PROVIDER_NPI_NUMBER as {{ dbt.type_string() }} ) as billing_npi
    , cast(NULL as {{ dbt.type_string() }} ) as facility_npi
    , cast(NULL as date) as paid_date
    , {{ cast_numeric('CLAIM_LINE_NCH_PAYMENT_AMOUNT') }} as paid_amount
    , {{ cast_numeric('NULL') }} as total_cost_amount
    , {{ cast_numeric('CLAIM_LINE_ALLOWED_CHARGES_AMOUNT') }} as allowed_amount
    , {{ cast_numeric('CLAIM_LINE_ALLOWED_CHARGES_AMOUNT') }} as charge_amount
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_type
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_1
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_2
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_3
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_4
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_5
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_6
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_7
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_8
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_9
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_10
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_11
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_12
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_13
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_14
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_15
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_16
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_17
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_18
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_19
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_20
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_21
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_22
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_23
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_24
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_25
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_1
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_2
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_3
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_4
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_5
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_6
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_7
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_8
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_9
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_10
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_11
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_12
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_13
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_14
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_15
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_16
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_17
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_18
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_19
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_20
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_21
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_22
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_23
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_24
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_25
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_type
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_1
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_2
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_3
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_4
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_5
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_6
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_7
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_8
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_9
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_10
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_11
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_12
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_13
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_14
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_15
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_16
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_17
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_18
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_19
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_20
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_21
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_22
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_23
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_24
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_25
    , cast(NULL as date) as procedure_date_1
    , cast(NULL as date) as procedure_date_2
    , cast(NULL as date) as procedure_date_3
    , cast(NULL as date) as procedure_date_4
    , cast(NULL as date) as procedure_date_5
    , cast(NULL as date) as procedure_date_6
    , cast(NULL as date) as procedure_date_7
    , cast(NULL as date) as procedure_date_8
    , cast(NULL as date) as procedure_date_9
    , cast(NULL as date) as procedure_date_10
    , cast(NULL as date) as procedure_date_11
    , cast(NULL as date) as procedure_date_12
    , cast(NULL as date) as procedure_date_13
    , cast(NULL as date) as procedure_date_14
    , cast(NULL as date) as procedure_date_15
    , cast(NULL as date) as procedure_date_16
    , cast(NULL as date) as procedure_date_17
    , cast(NULL as date) as procedure_date_18
    , cast(NULL as date) as procedure_date_19
    , cast(NULL as date) as procedure_date_20
    , cast(NULL as date) as procedure_date_21
    , cast(NULL as date) as procedure_date_22
    , cast(NULL as date) as procedure_date_23
    , cast(NULL as date) as procedure_date_24
    , cast(NULL as date) as procedure_date_25
    , 'medicare cclf' as data_source
from {{ source('medicare_cclf','C_6')}}