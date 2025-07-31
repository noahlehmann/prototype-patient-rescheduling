from logging import getLogger

import kubernetes.client
from kubernetes import client


LOG = getLogger(__name__)


def build_drain_options() -> str:
    options = []
    options.append("--ignore-daemonsets")
    options.append("--delete-emptydir-data")
    return " ".join(options)


def create_drain_job(node_name: str, service_account: str, label: str) -> client.V1Job:
    """
    Create a Kubernetes Job to drain a node.

    :param service_account: SA with drain permissions
    :param node_name: Name of the node to drain.
    :return: A V1Job object representing the job.
    """
    container = client.V1Container(
        name=f"drain-node",
        image="bitnami/kubectl:1.31.4",
        command=["/bin/sh", "-e", "-c"],
        args=[
            f"""
            kubectl drain {node_name} {build_drain_options()}
            kubectl label node {node_name} '{label}=drained' --overwrite
            """
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
            affinity=client.V1Affinity(
                node_affinity=client.V1NodeAffinity(
                    required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
                        node_selector_terms=[
                            client.V1NodeSelectorTerm(
                                match_expressions=[
                                    client.V1NodeSelectorRequirement(
                                        key="kubernetes.io/hostname",
                                        operator="NotIn",
                                        values=[node_name])
                                ])
                        ])
                )
            ),
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
            name=f"drain-{node_name}"
        ),
        spec=job_spec
    )

    return job


def dispatch_drain_job(node_name: str, namespace: str, service_account: str, label: str):
    job_api = kubernetes.client.BatchV1Api()
    job = create_drain_job(node_name, service_account, label)
    try:
        job_api.create_namespaced_job(namespace=namespace, body=job)
        LOG.info(f"Job {job.metadata.name} created successfully.")
    except Exception as e:
        LOG.error(f"Failed to create job: {e}")
