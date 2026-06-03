#!/usr/bin/env python3

import os
from datetime import datetime, timezone

import certifi
import oci
from confluent_kafka import Consumer, TopicPartition


DEFAULT_NAMESPACE = "streaming_custom"
DEFAULT_OFFSET_LAG_METRIC = "consumer_group_offset_lag_total"


def env_flag(name):
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "y", "on"}


def region_from_bootstrap_servers():
    for server in os.environ["OCI_KAFKA_BOOTSTRAP_SERVERS"].split(","):
        endpoint = server.strip().split(":", 1)[0]
        parts = endpoint.split(".")
        for index, part in enumerate(parts):
            if part == "streaming" and index + 1 < len(parts):
                return parts[index + 1]
    return None


def load_auth():
    if env_flag("OCI_USE_INSTANCE_PRINCIPAL"):
        region = os.getenv("OCI_REGION") or region_from_bootstrap_servers()
        if not region:
            raise RuntimeError(
                "OCI_REGION must be set when using instance principals if the region "
                "cannot be derived from OCI_KAFKA_BOOTSTRAP_SERVERS."
            )
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        return {"region": region}, signer

    profile = os.getenv("OCI_CONFIG_PROFILE", "DEFAULT")
    return oci.config.from_file(profile_name=profile), None


def build_client(client_class, config, signer=None, **kwargs):
    if signer is None:
        return client_class(config, **kwargs)
    return client_class(config, signer=signer, **kwargs)


def build_kafka_consumer():
    return Consumer(
        {
            "bootstrap.servers": os.environ["OCI_KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["OCI_KAFKA_SASL_USERNAME"],
            "sasl.password": os.environ["OCI_KAFKA_SASL_PASSWORD"],
            "ssl.ca.location": certifi.where(),
            "group.id": "lag-inspector",
            "enable.auto.commit": False,
        }
    )


def build_monitoring_client(config, signer=None):
    return build_client(
        oci.monitoring.MonitoringClient,
        config,
        signer=signer,
        service_endpoint=f"https://telemetry-ingestion.{config['region']}.oraclecloud.com",
    )


def build_metric_data(namespace, compartment_id, name, dimensions, timestamp, value):
    return oci.monitoring.models.MetricDataDetails(
        namespace=namespace,
        compartment_id=compartment_id,
        name=name,
        dimensions=dimensions,
        metadata={"unit": "count"},
        datapoints=[
            oci.monitoring.models.Datapoint(
                timestamp=timestamp,
                value=float(value),
                count=1,
            )
        ],
    )


def post_lag_metrics(
    monitoring_client,
    stream,
    group_name,
    total_raw_offset_gap,
):
    namespace = os.getenv("OCI_CUSTOM_METRIC_NAMESPACE", DEFAULT_NAMESPACE)
    metric_name = os.getenv(
        "OCI_OFFSET_LAG_METRIC_NAME",
        DEFAULT_OFFSET_LAG_METRIC,
    )
    metric_compartment_id = os.getenv(
        "OCI_MONITORING_COMPARTMENT_ID",
        stream.compartment_id,
    )
    timestamp = datetime.now(timezone.utc)

    dimensions = {
        "streamId": stream.id,
        "streamName": stream.name,
        "groupName": group_name,
    }

    metric_data = [
        build_metric_data(
            namespace,
            metric_compartment_id,
            metric_name,
            dimensions,
            timestamp,
            total_raw_offset_gap,
        ),
    ]

    response = monitoring_client.post_metric_data(
        post_metric_data_details=oci.monitoring.models.PostMetricDataDetails(
            metric_data=metric_data,
            batch_atomicity="NON_ATOMIC",
        )
    )

    print(
        "Posted custom metric "
        f"namespace={namespace} "
        f"metric={metric_name} "
        f"status={response.status}"
    )


def main():
    stream_id = os.environ["OCI_STREAM_ID"]
    group_name = os.environ["OCI_GROUP_NAME"]
    config, signer = load_auth()

    admin_client = build_client(
        oci.streaming.StreamAdminClient,
        config,
        signer=signer,
    )
    stream = admin_client.get_stream(stream_id).data

    topic = stream.name
    messages_endpoint = stream.messages_endpoint

    stream_client = build_client(
        oci.streaming.StreamClient,
        config,
        signer=signer,
        service_endpoint=messages_endpoint,
    )
    group = stream_client.get_group(stream_id, group_name).data
    monitoring_client = build_monitoring_client(config, signer=signer)

    kafka = build_kafka_consumer()

    try:
        total_raw_offset_gap = 0

        print(f"stream={topic} group={group_name}")
        print("partition committed_offset end_offset raw_offset_gap")

        for reservation in sorted(group.reservations, key=lambda item: int(item.partition)):
            partition = int(reservation.partition)
            committed = int(reservation.committed_offset)

            _low, high = kafka.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=10,
            )
            end_offset = high

            raw_offset_gap = max(0, end_offset - committed)

            total_raw_offset_gap += raw_offset_gap

            print(
                f"{partition:>9} "
                f"{committed:>16} "
                f"{end_offset:>10} "
                f"{raw_offset_gap:>14}"
            )

        print()
        print(f"TOTAL raw_offset_gap={total_raw_offset_gap}")

        post_lag_metrics(
            monitoring_client,
            stream,
            group_name,
            total_raw_offset_gap,
        )
    finally:
        kafka.close()


if __name__ == "__main__":
    main()
