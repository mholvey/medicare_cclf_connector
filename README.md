[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Medicare Claims Connector

Check out the Tuva Project [Docs](http://thetuvaproject.com/)

Check out the Tuva Project [Data Models](https://docs.google.com/spreadsheets/d/1NuMEhcx6D6MSyZEQ6yk0LWU0HLvaeVma8S-5zhOnbcE/edit?usp=sharing)

Check out the [DAG](https://tuva-health.github.io/medicare_cclf_connector/#!/overview?g_v=1) for this dbt project

## Description
This connector transforms raw Medicare CCLF claims data into the Tuva Claims Input Layer which enables you to run most of the other components of the Tuva Project with very little effort.

## Pre-requisites
1. You have Medicare CCLF claims data loaded into a data warehouse
2. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse)

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

## Getting Started

By default this package looks for tables conforming to this data model.
By setting these variables in your own `dbt_project.yml`, you may override the default table names.

This is particularly useful if you have multiple claims data sets you wish to run through The Tuva Project.
**TODO fill out examples**

```yaml
vars:
   
  tuva__medicare_cclf_connector__beneficiary_demographics: beneficiary_demographics
  tuva__medicare_cclf_connector__parta_diagnosis_code: parta_diagnosis_code
  tuva__medicare_cclf_connector__parta_claims_header: parta_claims_header
  tuva__medicare_cclf_connector__parta_claims_revenue_center_detail: parta_claims_revenue_center_detail
  tuva__medicare_cclf_connector__partb_physicians: partb_physicians
  tuva__medicare_cclf_connector__partb_dme: partb_dme
  tuva__medicare_cclf_connector__parta_procedure_code: parta_procedure_code
```

## Contributions
Have an opinion on the mappings? Notice any bugs when installing and running the package? 
If so, we highly encourage and welcome contributions! 

Join the conversation on [Slack](https://tuvahealth.slack.com/ssb/redirect#/shared-invite/email)!  We'd love to hear from you on the #claims-preprocessing channel.

## Database Support
This package has been built and tested on:
- Snowflake
- Redshift
