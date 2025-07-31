import datetime
import logging
import os

import kopf
import kubernetes
from dateutil import parser
from drain import dispatch_drain_job


def load_config():
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.config_exception.ConfigException:
        # use $KUBECONFIG to set kubeconfig path
        kubernetes.config.load_kube_config()


load_config()
core = kubernetes.client.CoreV1Api()
custom = kubernetes.client.CustomObjectsApi()
policy = kubernetes.client.PolicyV1Api()
LOG = logging.getLogger("nmwo")
nmwo_namespace = os.getenv("NMWO_NAMESPACE", "default")
nmwo_service_account = os.getenv("NMWO_SERVICE_ACCOUNT", "default")


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    # todo config seems to have ne effect on kopf settings
    settings.posting.level = logging.WARNING
    settings.posting.enabled = False
    settings.scanning.disabled = True


@kopf.timer('nodemaintenancewindows', interval=60.0, initial_delay=5)
@kopf.on.update('nodemaintenancewindows')
@kopf.on.create('nodemaintenancewindows')
def check(name, spec, **_):
    cleanup_crds = True
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    node_name = spec["nodeName"]
    end = parser.isoparse(spec["end"])
    drain_by = parser.isoparse(spec.get('drainBy', spec["start"]))
    label = f"ki-awz.iisys.de/{name}"

    LOG.debug(f"Checking CRD {name} at {now}")

    if end < now and cleanup_crds:
        LOG.info(f"{name} scheduled on node {node_name} is in past, cleaning up")
        custom.delete_cluster_custom_object(
            group="de.iisys.ki-awz",
            version="v1alpha1",
            plural="nodemaintenancewindows",
            name=name
        )
        core.patch_node(
            name=node_name,
            body={"metadata": {"labels": {label: None}}}
        )

    node_labels = core.read_node(node_name).metadata.labels

    if label not in node_labels.keys():
        LOG.info(f"{name} scheduled on node {node_name}")
        core.patch_node(
            name=node_name,
            body={"metadata": {"labels": {label: "scheduled"}}}
        )

    node_labels = core.read_node(node_name).metadata.labels

    if drain_by <= now and label in node_labels.keys() and node_labels[label] != "drained":
        LOG.info(f"{name} requested drain at {drain_by}")
        dispatch_drain_job(
            node_name=node_name,
            namespace=nmwo_namespace,
            service_account=nmwo_service_account,
            label=label
        )


if __name__ == '__main__':
    kopf.run(
        clusterwide=True,
        standalone=True,
        liveness_endpoint="http://0.0.0.0:8080/healthz"
    )
