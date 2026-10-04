# Cloud Scheduler

The job is managed by Terraform (`tb-platform-infra/terraform/modules/scheduler`). These scripts
are the `gcloud` equivalent for ad-hoc use and show exactly what the HTTP target does: an
authenticated (`--oauth-service-account-email`) POST to the Dataflow `flexTemplates:launch` REST API.
