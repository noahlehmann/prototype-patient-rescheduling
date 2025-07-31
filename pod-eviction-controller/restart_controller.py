from logging import getLogger

import kubernetes.client
from kubernetes import client

LOG = getLogger(__name__)


def create_drain_job(controller_type: str, controller_name: str, controller_namespace: str,
                     service_account: str) -> client.V1Job:
    container = client.V1Container(
        name=f"drain-node",
        image="bitnami/kubectl:1.31.4",
        command=["/bin/sh", "-e", "-c"],
        args=[
            f"kubectl rollout restart {controller_type} {controller_name} -n {controller_namespace}"
        ],
        resources=client.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "128Mi"}
        )
    )

    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={
            "app": "node-maintenance-window-operator",
            "action": f"drain"
        }),
        spec=client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            service_account_name=service_account
        )
    )

    job_spec = client.V1JobSpec(
        template=template,
        backoff_limit=0,
        ttl_seconds_after_finished=10
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=f"restart-{controller_name}"
        ),
        spec=job_spec
    )

    return job


def dispatch_restart_job(controller_type: str, controller_name: str, controller_namespace: str, job_namespace: str,
                         service_account: str):
    job_api = kubernetes.client.BatchV1Api()
    job = create_drain_job(controller_type, controller_name, controller_namespace, service_account)
    try:
        job_api.create_namespaced_job(namespace=job_namespace, body=job)
        LOG.info(f"Job {job.metadata.name} created successfully.")
    except Exception as e:
        LOG.error(f"Failed to create job: {e}")
