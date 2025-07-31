import datetime
import logging
import os

import kopf
import kubernetes
from dateutil import parser
from restart_controller import dispatch_restart_job


def load_config():
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.config_exception.ConfigException:
        # use $KUBECONFIG to set kubeconfig path
        kubernetes.config.load_kube_config()


load_config()
core = kubernetes.client.CoreV1Api()
custom = kubernetes.client.CustomObjectsApi()
apps = kubernetes.client.AppsV1Api()
policy = kubernetes.client.PolicyV1Api()
LOG = logging.getLogger("pec")
maintenance_label = os.getenv("PEC_MAINTENANCE_LABEL", "ki-awz.iisys.de/maintenance-managed")
managed_annotation = os.getenv("PEC_ANNOTATION", "ki-awz.iisys.de/pod-eviction-managed")
interactive_annotation = os.getenv("PEC_ANNOTATION", "ki-awz.iisys.de/user-interactive")
pec_namespace = os.getenv("PEC_NAMESPACE", "default")
pec_service_account = os.getenv("PEC_SERVICE_ACCOUNT", "default")

ORPHAN = "orphan"
OTHER = "other"
REPLICA_SET = "replicaset"
STATEFUL_SET = "statefulset"
DEPLOYMENT = "deployment"


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    # todo config seems to have ne effect on kopf settings
    settings.posting.level = logging.WARNING
    settings.posting.enabled = False
    settings.scanning.disabled = True


def get_windows_for_node(name):
    return [{
        "name": w["metadata"]["name"],
        "start": parser.isoparse(w["spec"]["start"]),
        "end": parser.isoparse(w["spec"]["end"])
    } for w in custom.list_cluster_custom_object(
        group="de.iisys.ki-awz",
        version="v1alpha1",
        plural="nodemaintenancewindows"
    )["items"] if w["spec"]["nodeName"] == name]


def find_next_window(windows):
    next_window = None
    for w in windows:
        if next_window is None or next_window.start < w.start:
            next_window = w
    return next_window


def find_resource_controller(resource, namespace):
    controller = [o for o in resource.metadata.owner_references if o.controller]
    if len(controller) == 0:
        return None
    controller = controller[0]
    if controller.kind.lower() == REPLICA_SET:
        replicaset_name = controller.name
        replica_set = apps.read_namespaced_replica_set(name=replicaset_name, namespace=namespace)
        rs_controller = find_resource_controller(replica_set, namespace)
        if rs_controller is None:
            return controller
        return rs_controller
    return controller


@kopf.timer('pods', interval=300.0, initial_delay=5)
def check_preferred_affinities_for_pods_on_node(name, namespace, **_):
    pod = core.read_namespaced_pod(name=name, namespace=namespace)
    controller = find_resource_controller(pod, namespace)
    # not supported controller types
    if controller.kind.lower() not in [DEPLOYMENT, STATEFUL_SET]:
        return
    # not managed -> descheduler
    if not pod.metadata.annotations or not pod.metadata.annotations.get(managed_annotation, "false").lower() == 'true':
        return
    # interactive migration not supported
    if pod.metadata.annotations.get(interactive_annotation, "false").lower() == 'true':
        return
    # no affinities, doesn't matter
    affinity = pod.spec.affinity
    if (not affinity
            or not affinity.node_affinity
            or not affinity.node_affinity.preferred_during_scheduling_ignored_during_execution
            or len(affinity.node_affinity.preferred_during_scheduling_ignored_during_execution) < 1):
        return
    node_labels = core.read_node(pod.spec.node_name).metadata.labels
    pod_fits_node = True
    for pref_term in affinity.node_affinity.preferred_during_scheduling_ignored_during_execution:
        for expr in pref_term.preference.match_expressions:
            key = expr.key
            operator = expr.operator.lower()
            # See if label needs to be present
            if key not in node_labels.keys() and operator != "doesnotexist":
                pod_fits_node = False
            else:
                match operator:
                    # is first so key is not None after that
                    case "doesnotexist":
                        pod_fits_node = key not in node_labels.keys()
                    case "in":
                        pod_fits_node = node_labels[key] in expr.values
                    case "notin":
                        pod_fits_node = node_labels[key] not in expr.values
                    case "exists":
                        pod_fits_node = key in node_labels.keys()
                    case "gt":
                        pod_fits_node = node_labels[key] > expr.values[0]
                    case "lt":
                        pod_fits_node = node_labels[key] < expr.values[0]
            if not pod_fits_node:
                dispatch_restart_job(
                    controller_type=controller.kind.lower(),
                    controller_name=controller.name,
                    controller_namespace=namespace,
                    job_namespace=pec_namespace,
                    service_account=pec_service_account
                )
            # todo limitation: 2 same weight preferred, evicted if one is not ok, needs more logic


@kopf.timer('nodes', interval=60.0, initial_delay=5)
@kopf.on.update('nodes')
def check(name, **_):
    LOG.debug(f"Starting PEC check on node {name}.")
    # check if managed
    node_labels = core.read_node(name).metadata.labels
    if maintenance_label not in node_labels.keys() or not node_labels[maintenance_label].lower() == "true":
        LOG.debug(f"Node {name} skipped because it is not managed by PEC")
        return
    # check if maintenance scheduled
    windows = get_windows_for_node(name)
    if len(windows) == 0:
        LOG.debug(f"No maintenance windows defined for node {name}")
        return
    # check if currently not in window
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    next_window = find_next_window(windows)
    if now < next_window["start"]:
        LOG.debug(f"No current maintenance window for node {name}: Next starts {next_window['start']}")
        return
    if next_window["end"] < now:
        LOG.debug(
            f"Latest maintenance window {next_window['name']} for node {name} is already in past: Ended {next_window['end']}")
        return
    # take care of special pods
    pods = core.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={name}").items
    for pod in pods:
        if (not pod.metadata.annotations
                or not pod.metadata.annotations.get(managed_annotation, "false").lower() == "true"):
            continue
        pod_name = pod.metadata.name
        pod_namespace = pod.metadata.namespace
        LOG.debug(f"Found managed pod {pod_name} in namespace {pod_namespace} on node {name} scheduled for maintenance")
        # if non user interactive
        if pod.metadata.annotations.get(interactive_annotation, "false").lower() == "true":
            # currently no way to preserve user interaction
            LOG.debug(
                f"Migrating user interactive pod {pod_name} in namespace {pod_namespace} is currently unsupported")
            continue
        controller = find_resource_controller(pod, pod_namespace)
        # if can restart
        if controller.kind.lower() in [DEPLOYMENT, STATEFUL_SET]:
            LOG.info(f"Attempting to migrate {pod_name} in namespace {pod_namespace} controlled by {controller.kind}")
            dispatch_restart_job(
                controller_type=controller.kind.lower(),
                controller_name=controller.name,
                controller_namespace=pod_namespace,
                job_namespace=pec_namespace,
                service_account=pec_service_account
            )
            pass
    LOG.debug(f"Finished PEC check on node {name}")


if __name__ == '__main__':
    kopf.run(
        clusterwide=True,
        standalone=True,
        liveness_endpoint="http://0.0.0.0:8080/healthz"
    )
