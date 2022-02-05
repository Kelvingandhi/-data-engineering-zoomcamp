GCP Overview

Video
Project infrastructure modules in GCP:

    Google Cloud Storage (GCS): Data Lake
    BigQuery: Data Warehouse

(Concepts explained in Week 2 - Data Ingestion)
Initial Setup

For this course, we'll use a free version (upto EUR 300 credits).

    Create an account with your Google email ID
    Setup your first project if you haven't already
        eg. "DTC DE Course", and note down the "Project ID" (we'll use this later when deploying infra with TF)
    Setup service account & authentication for this project
        Grant Viewer role to begin with.
        Download service-account-keys (.json) for auth.
    Download SDK for local setup
    Set environment variable to point to your downloaded GCP keys:

    export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"

    # Refresh token/session, and verify authentication
    gcloud auth application-default login

Setup for Access

    IAM Roles for Service account:
        Go to the IAM section of IAM & Admin https://console.cloud.google.com/iam-admin/iam
        Click the Edit principal icon for your service account.
        Add these roles in addition to Viewer : Storage Admin + Storage Object Admin + BigQuery Admin

    Enable these APIs for your project:
        https://console.cloud.google.com/apis/library/iam.googleapis.com
        https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com

    Please ensure GOOGLE_APPLICATION_CREDENTIALS env-var is set.

    export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"

Terraform Workshop to create GCP Infra

Continue here: week_1_basics_n_setup/1_terraform_gcp/terraform