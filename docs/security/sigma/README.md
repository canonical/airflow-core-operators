# Charmed Airflow Sigma Rules

These experimental Sigma rules match structured security events currently emitted by Charmed Airflow charms. They are detections for the implemented event inventory, not a claim that every SSDLC event is available.

## Rules

| Rule | Product identifier | Events |
| --- | --- | --- |
| `charmed-airflow-api-server-lifecycle.yml` | `airflow.api-server` | `sys_startup`, `sys_shutdown`, `sys_restart`, `sys_crash` |
| `charmed-airflow-scheduler-lifecycle.yml` | `airflow.scheduler` | `sys_startup`, `sys_shutdown`, `sys_restart`, `sys_crash` |
| `charmed-airflow-dag-processor-lifecycle.yml` | `airflow.dag-processor` | `sys_startup`, `sys_shutdown`, `sys_restart`, `sys_crash` |
| `charmed-airflow-triggerer-lifecycle.yml` | `airflow.triggerer` | `sys_startup`, `sys_shutdown`, `sys_restart`, `sys_crash` |
| `charmed-airflow-coordinator-authz-admin.yml` | `airflow.coordinator` | `authz_admin` |

The ``appid`` and ``event`` fields are nested under ``owasp_event`` in the JSON
record. Preserve that object when forwarding logs; do not flatten away the nested
attributes. The Sigma rules use selectors such as ``owasp_event.appid`` and
``owasp_event.event``.

## Validate And Convert

Install Sigma CLI (`sigma-cli`, based on pySigma) in an isolated Python environment, then validate the rules and convert them for a supported backend:

```bash
pipx install sigma-cli
sigma check docs/security/sigma
sigma convert -t <backend> -p <pipeline> docs/security/sigma
```

Use a backend and pipeline supported by your SIEM. Review generated queries against representative Charmed Airflow JSON logs before enabling alerts; field names and string-prefix behavior vary by backend.

## Ingest

Forward Juju unit logs to your SIEM using your organization's log collector. Loki, OpenSearch, and Wazuh require deployment-specific collection and field-parsing configuration; these rules do not configure log shipping. The charm events describe transitions observed by the charm, not all Kubernetes restarts or process exits. Correlate `actor: unattributed` administrative records with Juju audit records to identify the human initiator.

The rules are marked `experimental`. Tune expected deployment, maintenance, and recovery activity before paging on matches. Login success/failure, token lifecycle, and authorization-denial events are not currently emitted by these charm hooks and are intentionally not covered by these rules.
