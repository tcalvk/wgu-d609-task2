# STEDI Human Balance Analytics — Implementation Plan

## Objective and scope

Complete [PBP-483](https://corsaire.atlassian.net/browse/PBP-483) using Amazon S3, AWS Glue/Glue Studio, PySpark, and Amazon Athena. Deliver a validated lakehouse for machine-learning training, including both stand-out enhancements:

1. Exclude accelerometer readings collected before the customer granted research consent.
2. Remove customer and device identifiers from the final training table.

AWS access is available. This repository currently has no implementation. This document is the implementation guide; AWS setup, code, job execution, and evidence collection remain to be completed.

The scope does not include training a machine-learning model or building an application.

## Deliverables

```text
PLAN.md
README.md
sql/
    customer_landing.sql
    accelerometer_landing.sql
    step_trainer_landing.sql
    validation.sql
glue/
    customer_landing_to_trusted.py
    accelerometer_landing_to_trusted.py
    customer_trusted_to_curated.py
    step_trainer_trusted.py
    machine_learning_curated.py
screenshots/
    ... actual Athena evidence ...
```

- Execute landing DDL and validation SQL in Athena.
- Execute the five Python/PySpark scripts as AWS Glue jobs.
- Store actual data files in S3 and table metadata in the Glue Data Catalog.
- Save exported Glue Studio scripts in this repository.
- Do not commit AWS credentials, secrets, or raw customer data.

## Acceptance targets: stand-out submission

| Zone | Table | Expected rows |
| --- | --- | ---: |
| Landing | `customer_landing` | 956 |
| Landing | `accelerometer_landing` | 81,273 |
| Landing | `step_trainer_landing` | 28,680 |
| Trusted | `customer_trusted` | 482 |
| Trusted | `accelerometer_trusted` | 32,025 |
| Curated | `customers_curated` | 464 |
| Trusted | `step_trainer_trusted` | 14,460 |
| Curated | `machine_learning_curated` | 34,437 |

Use the ticket's stand-out counts, not the baseline counts or conflicting intermediate results in the upstream README. Investigate discrepancies rather than modifying data arbitrarily to reach a count.

The rubric alternates between `customer_curated` and `customers_curated`. Use `customers_curated` consistently and document that mapping in the README.

## Step 1 — Inspect the source and establish schemas

- [ ] Obtain the datasets from the [Udacity starter directory](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter).
- [ ] Confirm all JSON field names and types for customer, accelerometer, and step-trainer records.
- [ ] Inspect how missing research consent is represented: absent field, null, empty string, or another value.
- [ ] Confirm the actual consent field spelling, expected to be `shareWithResearchAsOfDate`; the ticket includes a spelling inconsistency.
- [ ] Confirm the email/user join fields and serial-number fields.
- [ ] Confirm timestamp units and compatible numeric types, using sufficiently wide types for epoch values.
- [ ] Check source file completeness and landing record counts.
- [ ] Record schemas and join assumptions in the README.

**Exit criterion:** Table definitions and transformations are based on inspected source fields, not inferred names alone.

## Step 2 — Prepare the repository and configuration

- [ ] Create the deliverable directories and README.
- [ ] Agree on AWS region, bucket, Glue database, job names, and execution role.
- [ ] Define job arguments for environment-specific values such as database and output paths.
- [ ] Keep credentials out of scripts and use the AWS execution role.
- [ ] Document the stand-out behavior and expected counts.

**Exit criterion:** The repository has a documented structure and consistent configuration strategy.

## Step 3 — Configure AWS resources

- [ ] Use one AWS region for the project resources.
- [ ] Create or select the project S3 bucket with separate prefixes:

```text
landing/customer/
landing/accelerometer/
landing/step_trainer/
trusted/customer/
trusted/accelerometer/
trusted/step_trainer/
curated/customers/
curated/machine_learning/
athena-results/
scripts/
```

- [ ] Create a Glue Data Catalog database.
- [ ] Configure the Athena workgroup/query-results location.
- [ ] Configure the Glue role for the required S3, catalog, and logging access, plus encryption permissions if applicable.
- [ ] Select modest Glue resources and monitor the course's $25 budget.
- [ ] Stop Glue preview sessions promptly; do not leave interactive sessions running.

**Exit criterion:** Athena can run a query and Glue has the required access to project locations.

## Step 4 — Upload and register landing data

**Files:** `sql/customer_landing.sql`, `sql/accelerometer_landing.sql`, `sql/step_trainer_landing.sql`.

- [ ] Upload the complete datasets into their corresponding landing prefixes.
- [ ] Write external-table DDL with every source field, appropriate types, a suitable JSON SerDe, and correct S3 locations.
- [ ] Run the DDL in Athena to register all three tables in the Glue Catalog.
- [ ] Query sample records to verify parsing and types.
- [ ] Verify counts: customer 956; accelerometer 81,273; step trainer 28,680.
- [ ] Query customer records with missing research consent.
- [ ] Capture screenshots showing query text and results. Include the required `customer_landing`, `accelerometer_landing`, and `step_trainer_landing` screenshot names, with additional count/consent screenshots as needed.

**Exit criterion:** All landing counts and schemas are correct, with saved evidence.

## Step 5 — Create customer trusted data

**File:** `glue/customer_landing_to_trusted.py`.

- [ ] Create a Glue Studio job with a customer landing S3 source as required by the rubric.
- [ ] Use a SQL Query transformation to retain customers with valid research consent, handling the missing-value representations confirmed in Step 1.
- [ ] Preserve the consent date for downstream checks.
- [ ] Write to the trusted customer prefix and register `customer_trusted`.
- [ ] Enable catalog table creation and subsequent schema/partition updates; verify the exported script includes the corresponding sink settings.
- [ ] Save/export the script to the repository and run the job.
- [ ] Verify 482 rows and zero missing consent values in Athena.
- [ ] Save `customer_trusted` screenshots, including count and consent evidence.

**Exit criterion:** Trusted customers contain only consenting customers and match the expected count.

## Step 6 — Create consent-aware accelerometer trusted data

**File:** `glue/accelerometer_landing_to_trusted.py`.

- [ ] Read accelerometer landing data using the required S3 source and read trusted customers from the catalog.
- [ ] Use a SQL inner join between accelerometer user and customer email.
- [ ] Compare timestamps in compatible units and retain readings at or after consent:

```sql
reading_timestamp >= research_consent_timestamp
```

- [ ] Include equality: the stand-out instruction excludes readings *prior to* consent.
- [ ] Select only accelerometer columns; do not carry customer PII into this table beyond the accelerometer source's user field needed for downstream eligibility checks.
- [ ] Write `accelerometer_trusted` to its S3 prefix and create/update the catalog schema.
- [ ] Save/export and run the job.
- [ ] Verify 32,025 rows.
- [ ] Verify every user matches a trusted customer and no retained reading predates consent.
- [ ] Capture Athena evidence.

**Exit criterion:** All trusted accelerometer readings were collected with consent already in place.

**Limitation:** This implements the supplied consent-start rule. Do not claim to model later revocations without consent-history data and additional processing.

## Step 7 — Create curated customers

**File:** `glue/customer_trusted_to_curated.py`.

- [ ] Read `customer_trusted` and `accelerometer_trusted` from the catalog.
- [ ] Use a SQL inner join by email.
- [ ] Select only customer columns and ensure each eligible customer occurs once, not once per reading. Join against distinct eligible users or deduplicate the customer projection.
- [ ] Write `customers_curated` and create/update the catalog schema.
- [ ] Save/export and run the job.
- [ ] Verify 464 customers, no unexpected customer duplication, valid consent, and at least one eligible reading for every customer.
- [ ] Capture Athena evidence.

**Exit criterion:** Curated customers are exactly the consenting customers with eligible accelerometer data.

## Step 8 — Create trusted step-trainer data

**File:** `glue/step_trainer_trusted.py`.

- [ ] Read step-trainer landing data with the required S3 source and `customers_curated` from the catalog.
- [ ] Use a SQL inner join by serial number.
- [ ] Join against distinct eligible serial numbers to avoid multiplying readings due to the known repeated customer serial numbers.
- [ ] Select only step-trainer columns.
- [ ] Write `step_trainer_trusted` and create/update the catalog schema.
- [ ] Save/export and run the job.
- [ ] Verify 14,460 rows and confirm every serial number matches the curated eligible set.
- [ ] Capture Athena evidence.

**Exit criterion:** Trusted step-trainer records meet the rubric's serial-number eligibility rule without join inflation.

**Caution:** Serial numbers in customer data are known to be reused. Document this source limitation rather than claiming the join establishes an unambiguous customer-to-device identity.

## Step 9 — Create the de-identified training table

**File:** `glue/machine_learning_curated.py`.

- [ ] Read trusted step-trainer and accelerometer data from the catalog.
- [ ] Use the rubric's SQL inner join on step-trainer reading time and accelerometer timestamp.
- [ ] Inspect join cardinality; the output can contain multiple matches per timestamp. Do not introduce extra join keys or arbitrary deduplication to force a target count.
- [ ] Explicitly project an allowlist of training columns, using clear output names:
  - Matched reading timestamp.
  - Step-trainer distance.
  - Accelerometer X, Y, and Z measurements.
- [ ] Exclude email/user, customer name, phone number, birthday, device serial number, and any other customer/device identifiers. Do not replace email with a stable hash and describe that as anonymization.
- [ ] Do not deduplicate after removing identifiers: separate source matches may legitimately become identical after projection.
- [ ] Write `machine_learning_curated` and create/update the catalog schema.
- [ ] Save/export and run the job.
- [ ] Verify 34,437 rows, matching source timestamps, and an output schema containing only approved training columns.
- [ ] Capture final-table count, sample output, and schema evidence.

**Exit criterion:** The final dataset matches the expected count, contains paired readings, and excludes direct customer/device identifiers.

**Privacy caveat:** Field removal meets the requested enhancement but does not by itself establish legal anonymization or exemption from GDPR. Timestamps and movement patterns can remain linkable. Describe the output as de-identified, document retained fields and residual risks, and avoid claiming regulatory immunity.

## Step 10 — Validate the whole pipeline

**File:** `sql/validation.sql`.

- [ ] Add reusable count queries for all eight tables.
- [ ] Add landing missing-consent and trusted valid-consent checks.
- [ ] Add trusted accelerometer unmatched-user and pre-consent checks; expected violations: zero.
- [ ] Add curated-customer uniqueness and eligible-reading checks; expected violations: zero.
- [ ] Add trusted step-trainer serial eligibility checks; expected violations: zero.
- [ ] Validate final join cardinality and source timestamp equality before identifier projection.
- [ ] Verify the final schema against the training-column allowlist.
- [ ] Compare all counts with the acceptance table above.
- [ ] Ensure screenshots show the relevant SQL, selected database/table context, and actual query results. Never fabricate evidence.

**Exit criterion:** Every count and semantic/privacy check passes, with reproducible SQL and saved evidence.

## Execution and rerun procedure

Execute in this order, validating each stage before proceeding:

```text
Upload landing data and run landing DDL
  -> customer_landing_to_trusted
  -> accelerometer_landing_to_trusted
  -> customer_trusted_to_curated
  -> step_trainer_trusted
  -> machine_learning_curated
```

- Use full-batch processing with job bookmarks disabled unless an explicit incremental strategy is designed and tested.
- Glue output may append files. Before a full rerun, confirm the exact job-specific output prefix, ensure no job is writing to it, and remove only that derived output; never clear the bucket or landing data indiscriminately.
- If an upstream stage changes, rebuild its downstream dependents in order.
- Update or recreate catalog metadata only when necessary for schema changes; deleting a table alone does not delete its S3 data.
- Validate full Athena tables rather than relying on sampled Glue previews.
- Prefer SQL Query transformation nodes. Use catalog sources for downstream tables to avoid incomplete source inference.
- The rubric explicitly requests landing S3 source nodes. If those read incompletely, reconcile the source configuration with the rubric before switching to a catalog source, and document the decision.

## Step 11 — Review and submit

- [ ] Complete the README with architecture, configuration, schemas, join logic, stand-out rules, run order, counts, screenshots, and clean-rerun instructions.
- [ ] Verify all three DDL scripts include every source field with appropriate types.
- [ ] Verify all five Glue scripts are exported and correspond to the actual tested jobs.
- [ ] Verify catalog creation/schema-update options and the required source-node configuration.
- [ ] Review all code changes and resolve correctness/privacy issues before submission.
- [ ] Confirm no credentials, secrets, or raw PII datasets are tracked.
- [ ] Map each rubric requirement to code and evidence.
- [ ] Commit/push only when requested; verify the submitted repository is accessible to the evaluator.
- [ ] Stop preview sessions and remove temporary resources only after evidence is saved and resources are no longer needed.

## Definition of done

The repository contains the three landing SQL scripts, five tested Glue scripts, reusable validation queries, actual Athena screenshots, and a reproducible README. All stand-out row counts and consent/eligibility checks pass. The final training table excludes direct customer and device identifiers, with privacy limitations documented.
