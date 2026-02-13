# Terraform module for Airflow Triggerer

This module deploys the Airflow Triggerer charm using the [Terraform juju provider](https://github.com/juju/terraform-provider-juju/). For provider details, see the [documentation](https://registry.terraform.io/providers/juju/juju/latest/docs).

## Requirements
- Terraform >= 1.12.2
- Provider: `juju` >= 1.0.0
- A Juju model must exist (see [Usage](#usage))

## API

### Inputs
The module offers the following configurable inputs:

| Name | Type | Description | Required |
| - | - | - | - |
| `app_name` | string | Name of the deployed application | False |
| `units` | number | Number of units to deploy | False |
| `model_uuid` | string | UUID of the model that the charm is deployed on | True |
| `revision` | number | Revision number of the charm name | False |
| `channel` | string | Channel that the charm is deployed from | False |
| `constraints` | string | Constraints to deploy the charm with | False |
| `config` | map(string) | Map of the charm configuration options | False |

### Outputs
Upon applied, the module exports the following outputs:

| Name | Description |
| - | - |
| `application` | Deployed application object |
| `provides` | Map of `provides` endpoints |
| `requires` | Map of `requires` endpoints |

## Usage

This module is intended to be used as part of a higher-level module. When defining one, ensure that Terraform is aware of the `juju_model` dependency of the charm module. There are two options to do so:

### Define a `juju_model` resource
Define a `juju_model` resource and pass to the `model_uuid` input a reference to the `juju_model` resource's UUID. For example:

```
resource "juju_model" "testing" {
  name = "airflow"
}

module "airflow-triggerer-k8s" {
  source     = "<path-to-this-directory>"
  model_uuid = juju_model.testing.uuid
}
```

### Define a `data` source
Define a `data` source and pass to the `model_uuid` input a reference to the `data.juju_model` resource's UUID. This will enable Terraform to look for a `juju_model` resource with a UUID attribute equal to the one provided, and apply only if this is present. Otherwise, it will fail before applying anything.

```
data "juju_model" "testing" {
  uuid = var.model_uuid
}

module "airflow-triggerer-k8s" {
  source     = "<path-to-this-directory>"
  model_uuid = data.juju_model.testing.uuid
}
```
