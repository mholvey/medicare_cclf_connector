
    select
          cast(CURRENT_CLAIM_UNIQUE_IDENTIFIER as {{ dbt.type_string() }} ) as cur_clm_uniq_id
        , cast(MEDICARE_BENEFICIARY_IDENTIFIER as {{ dbt.type_string() }} ) as bene_mbi_id
        {# , cast(bene_hic_num as {{ dbt.type_string() }} ) as bene_hic_num #}
        , cast(CLAIM_TYPE_CODE as {{ dbt.type_string() }} ) as clm_type_cd
        , cast(CLAIM_PRODUCT_TYPE_CODE as {{ dbt.type_string() }} ) as clm_prod_type_cd
        , cast(CLAIM_VALUE_SEQUENCE_NUMBER as {{ dbt.type_string() }} ) as clm_val_sqnc_num
        , cast(DIAGNOSIS_CODE as {{ dbt.type_string() }} ) as clm_dgns_cd
        {# , cast(bene_eqtbl_bic_hicn_num as {{ dbt.type_string() }} ) as bene_eqtbl_bic_hicn_num #}
        , cast(PROVIDER_OSCAR_NUMBER as {{ dbt.type_string() }} ) as prvdr_oscar_num
        , cast(CLAIM_FROM_DATE as {{ dbt.type_string() }} ) as clm_from_dt
        , cast(CLAIM_THRU_DATE as {{ dbt.type_string() }} ) as clm_thru_dt
        , cast(CLAIM_PRESENT_ON_ADMISSION_INDICATOR as {{ dbt.type_string() }} ) as clm_poa_ind
        , cast(ICD_VERSION_INDICATOR as {{ dbt.type_string() }} ) as dgns_prcdr_icd_ind
    from {{ source('medicare_cclf','C_4') }}
