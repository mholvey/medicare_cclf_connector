/*
  This model takes in eligibility data on the member month grain and converts
  it to enrollment date spans using row number and lag to account for
  continuous enrollment and gaps in coverage.
*/

with demographics as (

    select
          MEDICARE_BENEFICIARY_IDENTIFIER as bene_mbi_id
        , cast(BENEFICIARY_SEX_CODE as {{ dbt.type_string() }} ) as bene_sex_cd
        , cast(BENEFICIARY_RACE_CODE as {{ dbt.type_string() }} ) as bene_race_cd
        , BENEFICIARY_DATE_OF_BIRTH
        , BENEFICIARY_DEATH_DATE
        , {{ try_to_cast_date('BENE_ENTITLEMENT_PART_A_BEGIN_DATE', 'YYYY-MM-DD') }} as bene_member_month
        {# , {{ try_to_cast_date('bene_member_month', 'YYYY-MM-DD') }} as bene_member_month #}
        , BENEFICIARY_DUAL_STATUS_CODE as bene_dual_stus_cd
        , BENEFICIARY_MEDICARE_STATUS_CODE as bene_mdcr_stus_cd
        , BENEFICIARY_ORIGINAL_ENTITLEMENT_REASON_CODE as bene_orgnl_entlmt_rsn_cd
        , BENEFICIARY_FIRST_NAME as bene_1st_name
        , BENEFICIARY_LAST_NAME as bene_last_name
        , BENEFICIARY_DERIVED_MAILING_LINE_ONE_ADDRESS as bene_line_1_adr
        , BENEFICIARY_ZIP_CODE as geo_zip_plc_name
        , cast(BENEFICIARY_FIPS_STATE_CODE as bene_fips_state_cd as {{ dbt.type_string() }} ) as bene_fips_state_cd
        , BENEFICIARY_ZIP_CODE as bene_zip_cd
    from {{ source('medicare_cclf','C_8') }}

),

fips_state as (

    select * from {{ ref('terminology__ansi_fips_state') }}

),

add_row_num as (

    select *
         , row_number() over (
             partition by bene_mbi_id
             order by bene_member_month
           ) as row_num
    from demographics

),

add_lag_enrollment as (

    select
          bene_mbi_id
        , bene_member_month
        , row_num
        , lag(bene_member_month) over (
            partition by bene_mbi_id
            order by row_num
          ) as lag_enrollment
    from add_row_num

),

calculate_lag_diff as (

    select
          bene_mbi_id
        , bene_member_month
        , row_num
        , lag_enrollment
        , {{ datediff('lag_enrollment', 'bene_member_month', 'month') }} as lag_diff
    from add_lag_enrollment

),

calculate_gaps as (

     select
          bene_mbi_id
        , bene_member_month
        , row_num
        , lag_enrollment
        , lag_diff
        , case
            when lag_diff > 1 then 1
            else 0
          end as gap_flag
    from calculate_lag_diff

),

calculate_groups as (

     select
          bene_mbi_id
        , bene_member_month
        , row_num
        , lag_enrollment
        , lag_diff
        , gap_flag
        , sum(gap_flag) over (
            partition by bene_mbi_id
            order by row_num
            rows between unbounded preceding and current row
          ) as row_group
    from calculate_gaps

),

enrollment_span as (

    select
          bene_mbi_id
        , row_group
        , min(bene_member_month) as enrollment_start_date
        , max(bene_member_month) as enrollment_end_date_max
        , {{ last_day('max(bene_member_month)', 'month') }} as enrollment_end_date_last
    from calculate_groups
    group by bene_mbi_id, row_group

),

joined as (

    select
          cast(enrollment_span.bene_mbi_id as {{ dbt.type_string() }} ) as patient_id
        , cast(enrollment_span.bene_mbi_id as {{ dbt.type_string() }} ) as member_id
        , case demographics.bene_sex_cd
            when '0' then 'unknown'
            when '1' then 'male'
            when '2' then 'female'
          end as gender
        , case demographics.bene_race_cd
            when '0' then 'unknown'
            when '1' then 'white'
            when '2' then 'black'
            when '3' then 'other'
            when '4' then 'asian'
            when '5' then 'hispanic'
            when '6' then 'north american native'
          end as race
        , {{ try_to_cast_date('demographics.bene_dob', 'YYYY-MM-DD') }} as birth_date
        , {{ try_to_cast_date('demographics.bene_death_dt', 'YYYY-MM-DD') }} as death_date
        , cast(case
               when demographics.bene_death_dt is null then 0
               else 1
          end as integer) as death_flag
        , enrollment_span.enrollment_start_date
        , enrollment_span.enrollment_end_date_last as enrollment_end_date
        , 'medicare' as payer
        , 'medicare' as payer_type
        , cast(bene_orgnl_entlmt_rsn_cd as {{ dbt.type_string() }} ) as original_reason_entitlement_code
        , cast(demographics.bene_dual_stus_cd as {{ dbt.type_string() }} ) as dual_status_code
        , cast(demographics.bene_mdcr_stus_cd as {{ dbt.type_string() }} ) as medicare_status_code
        , cast(demographics.bene_1st_name as {{ dbt.type_string() }} ) as first_name
        , cast(demographics.bene_last_name as {{ dbt.type_string() }} ) as last_name
        , cast(demographics.bene_line_1_adr as {{ dbt.type_string() }} ) as address
        , cast(demographics.geo_zip_plc_name as {{ dbt.type_string() }} ) as city
        , cast(fips_state.ansi_fips_state_name as {{ dbt.type_string() }} ) as state
        , cast(demographics.bene_zip_cd as {{ dbt.type_string() }} ) as zip_code
        , cast(NULL as {{ dbt.type_string() }} ) as phone
        , 'medicare cclf' as data_source
    from enrollment_span
         left join demographics
            on enrollment_span.bene_mbi_id = demographics.bene_mbi_id
            and enrollment_span.enrollment_end_date_max = demographics.bene_member_month
         left join fips_state
            on demographics.bene_fips_state_cd = fips_state.ansi_fips_state_code

)

select * from joined