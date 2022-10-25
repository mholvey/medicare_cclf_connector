{% macro get_variable_from_all_scopes(variable) %}
{#
This macro makes it east to access variables across various scopes.
#}

{% set package_var = var(variable) if var(variable, false) %}
{% set global_var = var("admind_dbt_ga4")[variable] if var("admind_dbt_ga4", false) %}
{% set desired_var = package_var if package_var else global_var %}
{{ return(desired_var) }}

{% endmacro %}
