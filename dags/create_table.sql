CREATE OR REPLACE TABLE "EL_MATILLION_RAW"."RAW_CSV"."dimcurrentlossrun" (
	"﻿claim_number" NUMBER(38, 0),
	"prior_claim_number" VARCHAR,
	"date_of_loss_claim_desc" VARCHAR,
	"claimant_name" VARCHAR,
	"coverage" VARCHAR,
	"birth_date" VARCHAR,
	"sex" VARCHAR,
	"claimant_status_desc" VARCHAR,
	"litigated" BOOLEAN,
	"litigation_status" VARCHAR,
	"state_code" VARCHAR,
	"incident_claims_made_date" TIMESTAMP_NTZ,
	"report_date" TIMESTAMP_NTZ,
	"closed_date" TIMESTAMP_NTZ,
	"reopened_date" TIMESTAMP_NTZ,
	"claimant_type_desc" VARCHAR,
	"ulae_expense_amount" NUMBER(38, 2),
	"ulae_reserves" NUMBER(38, 2),
	"ulae_incurred" NUMBER(38, 2),
	"paid_indemnity" NUMBER(38, 2),
	"paid_expense" NUMBER(38, 2),
	"paid" NUMBER(38, 2),
	"incurred_indemnity" NUMBER(38, 2),
	"incurred_expense" NUMBER(38, 2),
	"incurred" NUMBER(38, 2),
	"outstanding_indemnity" NUMBER(38, 2),
	"outstanding_expense" NUMBER(38, 2),
	"outstanding" NUMBER(38, 2),
	"client_code" NUMBER(38, 0),
	"insurer_name" VARCHAR,
	"insured_name1" VARCHAR,
	"policy_number" VARCHAR,
	"policy_period_desc" VARCHAR,
	"policy_year" VARCHAR,
	"closed_claimant" BOOLEAN,
	"open_claimant" BOOLEAN,
	"closed_lit_claimant" BOOLEAN,
	"open_lit_claimant" BOOLEAN
);