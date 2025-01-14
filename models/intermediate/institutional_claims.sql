with claim_line as (
select distinct
    a.CURRENT_CLAIM_UNIQUE_IDENTIFIER as claim_id
,   b.CLAIM_LINE_NUMBER as claim_line_number
,   row_number() over(partition by a.CURRENT_CLAIM_UNIQUE_IDENTIFIER order by b.CLAIM_LINE_NUMBER) as claim_row_number
from {{ source('medicare_cclf','C_1')}} a
left join {{ source('medicare_cclf','C_2')}} b
    on a.CURRENT_CLAIM_UNIQUE_IDENTIFIER = b.CURRENT_CLAIM_UNIQUE_IDENTIFIER
)

, add_header_paid_amount as (
select 
    a.claim_id
,   a.claim_line_number
,   a.claim_row_number
,   b.CLAIM_PAYMENT_AMOUNT as paid_amount
from claim_line a
inner join {{ source('medicare_cclf','C_1')}} b
    on a.claim_id = b.CURRENT_CLAIM_UNIQUE_IDENTIFIER
where a.claim_row_number = 1
)

, claim_line_a as (
select
    a.claim_id
,   a.claim_line_number
,   a.claim_row_number
,   b.paid_amount
from claim_line a
left join add_header_paid_amount b
    on a.claim_id = b.claim_id
    and a.claim_row_number = b.claim_row_number
)

select
      cast(a.claim_id as {{ dbt.type_string() }} ) as claim_id
    , cast(a.claim_line_number as integer) as claim_line_number
    , 'institutional' as claim_type
    , cast(h.MEDICARE_BENEFICIARY_IDENTIFIER as {{ dbt.type_string() }} ) as patient_id
    , cast(h.MEDICARE_BENEFICIARY_IDENTIFIER as {{ dbt.type_string() }} ) as member_id
    , {{ try_to_cast_date('h.CLAIM_FROM_DATE', 'YYYY-MM-DD') }} as claim_start_date
    , {{ try_to_cast_date('h.CLAIM_THRU_DATE', 'YYYY-MM-DD') }} as claim_end_date
    , cast(NULL as date) as claim_line_start_date
    , cast(NULL as date) as claim_line_end_date
    , cast(NULL as date) as admission_date
    , cast(NULL as date) as discharge_date
    , cast(h.CLAIM_ADMISSION_SOURCE_CODE as {{ dbt.type_string() }} ) as admit_source_code
    , cast(h.CLAIM_ADMISSION_TYPE_CODE as {{ dbt.type_string() }} ) as admit_type_code
    , lpad(cast(h.BENEFICIARY_PATIENT_STATUS_CODE as {{ dbt.type_string() }} ),2, '0') as discharge_disposition_code
    , cast(NULL as {{ dbt.type_string() }} ) as place_of_service_code
    , cast(h.CLAIM_BILL_FACILITY_TYPE_CODE as {{ dbt.type_string() }} )
        || cast(h.CLAIM_BILL_CLASSIFICATION_CODE as {{ dbt.type_string() }} )
        || cast(h.CLAIM_BILL_FREQUENCY_CODE as {{ dbt.type_string() }} )
      as bill_type_code
    , cast(h.DIAGNOSIS_RELATED_GROUP_CODE  as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(null as {{ dbt.type_string() }} ) as apr_drg_code
    ,  lpad(cast(d.PRODUCT_REVENUE_CENTER_CODE as {{ dbt.type_string() }} ),4,'0') as revenue_center_code
    , cast(d.CLAIM_LINE_SERVICE_UNIT_QUANTITY as integer) as service_unit_quantity
    , cast(d.HCPCS_CODE as {{ dbt.type_string() }} ) as hcpcs_code
    , cast(d.HCPCS_FIRST_MODIFIER_CODE as {{ dbt.type_string() }} ) as hcpcs_modifier_1
    , cast(d.HCPCS_SECOND_MODIFIER_CODE as {{ dbt.type_string() }} ) as hcpcs_modifier_2
    , cast(d.HCPCS_THIRD_MODIFIER_CODE as {{ dbt.type_string() }} ) as hcpcs_modifier_3
    , cast(d.HCPCS_FOURTH_MODIFIER_CODE as {{ dbt.type_string() }} ) as hcpcs_modifier_4
    {# , cast(d.hcpcs_5_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_5 #}
    , cast(h.ATTENDING_PROVIDER_NPI_NUMBER as {{ dbt.type_string() }} ) as rendering_npi
    , cast(NULL as {{ dbt.type_string() }} ) as billing_npi
    , cast(h.FACILITY_PROVIDER_NPI_NUMBER as {{ dbt.type_string() }} ) as facility_npi
    , cast(NULL as date) as paid_date
    , {{ cast_numeric('a.paid_amount') }} as paid_amount
    , {{ cast_numeric('NULL') }} as total_cost_amount
    , {{ cast_numeric('NULL') }} as allowed_amount
    , {{ cast_numeric('h.TOTAL_CLAIM_CHARGE_AMOUNT') }} as charge_amount
    , case
        when cast(dx.dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) = '0' then 'icd-10-cm'
        when cast(dx.dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) = '9' then 'icd-9-cm'
        else cast(dx.dgns_prcdr_icd_ind as {{ dbt.type_string() }} )
      end as diagnosis_code_type
    , cast(dx.diagnosis_code_1 as {{ dbt.type_string() }} ) as diagnosis_code_1
    , cast(dx.diagnosis_code_2 as {{ dbt.type_string() }} ) as diagnosis_code_2
    , cast(dx.diagnosis_code_3 as {{ dbt.type_string() }} ) as diagnosis_code_3
    , cast(dx.diagnosis_code_4 as {{ dbt.type_string() }} ) as diagnosis_code_4
    , cast(dx.diagnosis_code_5 as {{ dbt.type_string() }} ) as diagnosis_code_5
    , cast(dx.diagnosis_code_6 as {{ dbt.type_string() }} ) as diagnosis_code_6
    , cast(dx.diagnosis_code_7 as {{ dbt.type_string() }} ) as diagnosis_code_7
    , cast(dx.diagnosis_code_8 as {{ dbt.type_string() }} ) as diagnosis_code_8
    , cast(dx.diagnosis_code_9 as {{ dbt.type_string() }} ) as diagnosis_code_9
    , cast(dx.diagnosis_code_10 as {{ dbt.type_string() }} ) as diagnosis_code_10
    , cast(dx.diagnosis_code_11 as {{ dbt.type_string() }} ) as diagnosis_code_11
    , cast(dx.diagnosis_code_12 as {{ dbt.type_string() }} ) as diagnosis_code_12
    , cast(dx.diagnosis_code_13 as {{ dbt.type_string() }} ) as diagnosis_code_13
    , cast(dx.diagnosis_code_14 as {{ dbt.type_string() }} ) as diagnosis_code_14
    , cast(dx.diagnosis_code_15 as {{ dbt.type_string() }} ) as diagnosis_code_15
    , cast(dx.diagnosis_code_16 as {{ dbt.type_string() }} ) as diagnosis_code_16
    , cast(dx.diagnosis_code_17 as {{ dbt.type_string() }} ) as diagnosis_code_17
    , cast(dx.diagnosis_code_18 as {{ dbt.type_string() }} ) as diagnosis_code_18
    , cast(dx.diagnosis_code_19 as {{ dbt.type_string() }} ) as diagnosis_code_19
    , cast(dx.diagnosis_code_20 as {{ dbt.type_string() }} ) as diagnosis_code_20
    , cast(dx.diagnosis_code_21 as {{ dbt.type_string() }} ) as diagnosis_code_21
    , cast(dx.diagnosis_code_22 as {{ dbt.type_string() }} ) as diagnosis_code_22
    , cast(dx.diagnosis_code_23 as {{ dbt.type_string() }} ) as diagnosis_code_23
    , cast(dx.diagnosis_code_24 as {{ dbt.type_string() }} ) as diagnosis_code_24
    , cast(dx.diagnosis_code_25 as {{ dbt.type_string() }} ) as diagnosis_code_25
    , cast(dx.diagnosis_poa_1 as {{ dbt.type_string() }} ) as diagnosis_poa_1
    , cast(dx.diagnosis_poa_2 as {{ dbt.type_string() }} ) as diagnosis_poa_2
    , cast(dx.diagnosis_poa_3 as {{ dbt.type_string() }} ) as diagnosis_poa_3
    , cast(dx.diagnosis_poa_4 as {{ dbt.type_string() }} ) as diagnosis_poa_4
    , cast(dx.diagnosis_poa_5 as {{ dbt.type_string() }} ) as diagnosis_poa_5
    , cast(dx.diagnosis_poa_6 as {{ dbt.type_string() }} ) as diagnosis_poa_6
    , cast(dx.diagnosis_poa_7 as {{ dbt.type_string() }} ) as diagnosis_poa_7
    , cast(dx.diagnosis_poa_8 as {{ dbt.type_string() }} ) as diagnosis_poa_8
    , cast(dx.diagnosis_poa_9 as {{ dbt.type_string() }} ) as diagnosis_poa_9
    , cast(dx.diagnosis_poa_10 as {{ dbt.type_string() }} ) as diagnosis_poa_10
    , cast(dx.diagnosis_poa_11 as {{ dbt.type_string() }} ) as diagnosis_poa_11
    , cast(dx.diagnosis_poa_12 as {{ dbt.type_string() }} ) as diagnosis_poa_12
    , cast(dx.diagnosis_poa_13 as {{ dbt.type_string() }} ) as diagnosis_poa_13
    , cast(dx.diagnosis_poa_14 as {{ dbt.type_string() }} ) as diagnosis_poa_14
    , cast(dx.diagnosis_poa_15 as {{ dbt.type_string() }} ) as diagnosis_poa_15
    , cast(dx.diagnosis_poa_16 as {{ dbt.type_string() }} ) as diagnosis_poa_16
    , cast(dx.diagnosis_poa_17 as {{ dbt.type_string() }} ) as diagnosis_poa_17
    , cast(dx.diagnosis_poa_18 as {{ dbt.type_string() }} ) as diagnosis_poa_18
    , cast(dx.diagnosis_poa_19 as {{ dbt.type_string() }} ) as diagnosis_poa_19
    , cast(dx.diagnosis_poa_20 as {{ dbt.type_string() }} ) as diagnosis_poa_20
    , cast(dx.diagnosis_poa_21 as {{ dbt.type_string() }} ) as diagnosis_poa_21
    , cast(dx.diagnosis_poa_22 as {{ dbt.type_string() }} ) as diagnosis_poa_22
    , cast(dx.diagnosis_poa_23 as {{ dbt.type_string() }} ) as diagnosis_poa_23
    , cast(dx.diagnosis_poa_24 as {{ dbt.type_string() }} ) as diagnosis_poa_24
    , cast(dx.diagnosis_poa_25 as {{ dbt.type_string() }} ) as diagnosis_poa_25
    , case
        when cast(px.dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) = '0' then 'icd-10-pcs'
        when cast(px.dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) = '9' then 'icd-9-pcs'
        else cast(px.dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) end  procedure_code_type
    , cast(px.procedure_code_1 as {{ dbt.type_string() }} ) as procedure_code_1
    , cast(px.procedure_code_2 as {{ dbt.type_string() }} ) as procedure_code_2
    , cast(px.procedure_code_3 as {{ dbt.type_string() }} ) as procedure_code_3
    , cast(px.procedure_code_4 as {{ dbt.type_string() }} ) as procedure_code_4
    , cast(px.procedure_code_5 as {{ dbt.type_string() }} ) as procedure_code_5
    , cast(px.procedure_code_6 as {{ dbt.type_string() }} ) as procedure_code_6
    , cast(px.procedure_code_7 as {{ dbt.type_string() }} ) as procedure_code_7
    , cast(px.procedure_code_8 as {{ dbt.type_string() }} ) as procedure_code_8
    , cast(px.procedure_code_9 as {{ dbt.type_string() }} ) as procedure_code_9
    , cast(px.procedure_code_10 as {{ dbt.type_string() }} ) as procedure_code_10
    , cast(px.procedure_code_11 as {{ dbt.type_string() }} ) as procedure_code_11
    , cast(px.procedure_code_12 as {{ dbt.type_string() }} ) as procedure_code_12
    , cast(px.procedure_code_13 as {{ dbt.type_string() }} ) as procedure_code_13
    , cast(px.procedure_code_14 as {{ dbt.type_string() }} ) as procedure_code_14
    , cast(px.procedure_code_15 as {{ dbt.type_string() }} ) as procedure_code_15
    , cast(px.procedure_code_16 as {{ dbt.type_string() }} ) as procedure_code_16
    , cast(px.procedure_code_17 as {{ dbt.type_string() }} ) as procedure_code_17
    , cast(px.procedure_code_18 as {{ dbt.type_string() }} ) as procedure_code_18
    , cast(px.procedure_code_19 as {{ dbt.type_string() }} ) as procedure_code_19
    , cast(px.procedure_code_20 as {{ dbt.type_string() }} ) as procedure_code_20
    , cast(px.procedure_code_21 as {{ dbt.type_string() }} ) as procedure_code_21
    , cast(px.procedure_code_22 as {{ dbt.type_string() }} ) as procedure_code_22
    , cast(px.procedure_code_23 as {{ dbt.type_string() }} ) as procedure_code_23
    , cast(px.procedure_code_24 as {{ dbt.type_string() }} ) as procedure_code_24
    , cast(px.procedure_code_25 as {{ dbt.type_string() }} ) as procedure_code_25
    , {{ try_to_cast_date('px.procedure_date_1', 'YYYY-MM-DD') }} as procedure_date_1
    , {{ try_to_cast_date('px.procedure_date_2', 'YYYY-MM-DD') }} as procedure_date_2
    , {{ try_to_cast_date('px.procedure_date_3', 'YYYY-MM-DD') }} as procedure_date_3
    , {{ try_to_cast_date('px.procedure_date_4', 'YYYY-MM-DD') }} as procedure_date_4
    , {{ try_to_cast_date('px.procedure_date_5', 'YYYY-MM-DD') }} as procedure_date_5
    , {{ try_to_cast_date('px.procedure_date_6', 'YYYY-MM-DD') }} as procedure_date_6
    , {{ try_to_cast_date('px.procedure_date_7', 'YYYY-MM-DD') }} as procedure_date_7
    , {{ try_to_cast_date('px.procedure_date_8', 'YYYY-MM-DD') }} as procedure_date_8
    , {{ try_to_cast_date('px.procedure_date_9', 'YYYY-MM-DD') }} as procedure_date_9
    , {{ try_to_cast_date('px.procedure_date_10', 'YYYY-MM-DD') }} as procedure_date_10
    , {{ try_to_cast_date('px.procedure_date_11', 'YYYY-MM-DD') }} as procedure_date_11
    , {{ try_to_cast_date('px.procedure_date_12', 'YYYY-MM-DD') }} as procedure_date_12
    , {{ try_to_cast_date('px.procedure_date_13', 'YYYY-MM-DD') }} as procedure_date_13
    , {{ try_to_cast_date('px.procedure_date_14', 'YYYY-MM-DD') }} as procedure_date_14
    , {{ try_to_cast_date('px.procedure_date_15', 'YYYY-MM-DD') }} as procedure_date_15
    , {{ try_to_cast_date('px.procedure_date_16', 'YYYY-MM-DD') }} as procedure_date_16
    , {{ try_to_cast_date('px.procedure_date_17', 'YYYY-MM-DD') }} as procedure_date_17
    , {{ try_to_cast_date('px.procedure_date_18', 'YYYY-MM-DD') }} as procedure_date_18
    , {{ try_to_cast_date('px.procedure_date_19', 'YYYY-MM-DD') }} as procedure_date_19
    , {{ try_to_cast_date('px.procedure_date_20', 'YYYY-MM-DD') }} as procedure_date_20
    , {{ try_to_cast_date('px.procedure_date_21', 'YYYY-MM-DD') }} as procedure_date_21
    , {{ try_to_cast_date('px.procedure_date_22', 'YYYY-MM-DD') }} as procedure_date_22
    , {{ try_to_cast_date('px.procedure_date_23', 'YYYY-MM-DD') }} as procedure_date_23
    , {{ try_to_cast_date('px.procedure_date_24', 'YYYY-MM-DD') }} as procedure_date_24
    , {{ try_to_cast_date('px.procedure_date_25', 'YYYY-MM-DD') }} as procedure_date_25
    , 'medicare cclf' as data_source
from claim_line_a a
left join {{ source('medicare_cclf','C_1')}} h
  on a.claim_id = h.CURRENT_CLAIM_UNIQUE_IDENTIFIER
left join {{ source('medicare_cclf','C_2')}} d
	on a.claim_id = d.CURRENT_CLAIM_UNIQUE_IDENTIFIER
  and a.claim_line_number = d.CLAIM_LINE_NUMBER
left join {{ ref('procedure_pivot')}} px
	on cast(a.claim_id as {{ dbt.type_string() }} ) = cast(px.cur_clm_uniq_id as {{ dbt.type_string() }} )
left join {{ ref('diagnosis_pivot')}} dx
	on cast(a.claim_id as {{ dbt.type_string() }} ) = cast(dx.cur_clm_uniq_id as {{ dbt.type_string() }} )