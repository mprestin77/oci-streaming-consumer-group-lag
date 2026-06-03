# OCI Streaming Consumer Group Lag Scripts

This directory contains two Python scripts for checking OCI Streaming consumer group lag.

- `get-stream-gap.py`
  Reads the current consumer group offsets and Kafka high watermarks, then prints per-partition lag and total lag.
- `get-stream-gap-metrics.py`
  Does the same lag calculation and also publishes the total lag as an OCI Monitoring custom metric.

## What the scripts measure

Both scripts calculate `raw_offset_gap` as:

```text
end_offset - committed_offset
```

Where:

- `end_offset` is the Kafka high watermark for the partition
- `committed_offset` is the consumer group's committed offset for the partition

The printed `TOTAL raw_offset_gap` is the sum across all partitions.

If `TOTAL raw_offset_gap=0`, the consumer group is fully caught up.

## Prerequisites

- Python 3
- OCI config file in `~/.oci/config`
- An OCI profile with permission to read stream metadata and consumer group state
- For `get-stream-gap-metrics.py`, permission to publish OCI Monitoring custom metrics

Python packages:

```bash
pip install oci confluent-kafka certifi
```

## Required Environment Variables

Set these for both scripts:

```bash
export OCI_STREAM_ID='<stream_ocid>'
export OCI_GROUP_NAME='<consumer_group_name>'
export OCI_KAFKA_BOOTSTRAP_SERVERS='<bootstrap_host>:9092'
export OCI_KAFKA_SASL_USERNAME='<kafka_username>'
export OCI_KAFKA_SASL_PASSWORD='<kafka_password>'
```

Required meaning:

- `OCI_STREAM_ID`
  OCI Stream OCID
- `OCI_GROUP_NAME`
  Consumer group name to inspect
- `OCI_KAFKA_BOOTSTRAP_SERVERS`
  Kafka bootstrap endpoint for the stream pool
- `OCI_KAFKA_SASL_USERNAME`
  Kafka SASL username for OCI Streaming Kafka API
- `OCI_KAFKA_SASL_PASSWORD`
  Kafka auth token / password

## Optional Environment Variables

Optional for both scripts:

```bash
export OCI_CONFIG_PROFILE='DEFAULT'
```

- `OCI_CONFIG_PROFILE`
  OCI config profile name. Default: `DEFAULT`

Additional optional variables for `get-stream-gap-metrics.py`:

```bash
export OCI_CUSTOM_METRIC_NAMESPACE='streaming_custom'
export OCI_OFFSET_LAG_METRIC_NAME='consumer_group_offset_lag_total'
export OCI_MONITORING_COMPARTMENT_ID='<compartment_ocid>'
```

- `OCI_CUSTOM_METRIC_NAMESPACE`
  Custom Monitoring namespace. Default: `streaming_custom`
- `OCI_OFFSET_LAG_METRIC_NAME`
  Custom metric name. Default: `consumer_group_offset_lag_total`
- `OCI_MONITORING_COMPARTMENT_ID`
  Compartment where the custom metric is published. Default: the stream's compartment

## Usage

Run lag inspection only:

```bash
python3 get-stream-gap.py
```

Run lag inspection and publish a custom metric:

```bash
python3 get-stream-gap-metrics.py
```

## Example Output

```text
stream=my-stream group=group1
partition committed_offset end_offset raw_offset_gap
        0              120        120              0
        1              200        203              3
        2               55         59              4

TOTAL raw_offset_gap=7
```

## Custom Metric Published

`get-stream-gap-metrics.py` publishes one custom metric by default:

- Namespace: `streaming_custom`
- Metric name: `consumer_group_offset_lag_total`

Metric dimensions:

- `streamId`
- `streamName`
- `groupName`

If the metric already exists, each script run adds a new datapoint to the same metric stream.

## Creating an OCI Alarm

You can create an OCI Monitoring alarm on the custom metric and trigger the alarm when the configured threshold is exceeded.

Typical flow:

1. Run `get-stream-gap-metrics.py` so the custom metric is published.
2. Open OCI Monitoring and locate the metric in the selected compartment and namespace.
3. Create a threshold alarm for `consumer_group_offset_lag_total`.
4. Configure the threshold, severity, and notification destination.

Useful OCI documentation:

- [Publishing Custom Metrics](https://docs.oracle.com/en-us/iaas/Content/bigdata/metrics-view-publish-custom-overview.htm)
- [Creating a Basic Alarm](https://docs.oracle.com/en-us/iaas/Content/Monitoring/Tasks/create-alarm-basic.htm)
- [Creating a Threshold Alarm](https://docs.oracle.com/en-us/iaas/Content/Monitoring/Tasks/create-alarm-threshold.htm)
- [Custom Metrics Walkthrough](https://docs.oracle.com/en-us/iaas/Content/Monitoring/Tasks/custom-metrics-walkthrough.htm)

Example screenshots:

The screenshots below intentionally show only the first part of the `streamId`.

Alarm definition:

![Streaming alarm definition](images/Streaming%20alarm%20definition.png)

Alarm fired:

![Streaming alarm fired](images/Streaming%20alarm%20filred.png)

## Scheduling Periodic Execution

`get-stream-gap-metrics.py` updates the custom metric only when the script runs. To keep the metric current for dashboards and alarms, run the script periodically.

One practical option is to add it to `crontab` on Linux.

Example: run every 5 minutes

```bash
*/5 * * * * /usr/bin/env bash -lc 'cd /Users/mprestin/scripts/streaming && source ./set-env.sh && python3 get-stream-gap-metrics.py >> /tmp/get-stream-gap-metrics.log 2>&1'
```

You can install the cron entry with:

```bash
crontab -e
```

## Example Environment Setup

```bash
export OCI_STREAM_ID='ocid1.stream.oc1.iad.example'
export OCI_GROUP_NAME='group1'
export OCI_KAFKA_BOOTSTRAP_SERVERS='cell-1.streaming.us-ashburn-1.oci.oraclecloud.com:9092'
export OCI_KAFKA_SASL_USERNAME='tenant/user/ocid1.streampool.oc1.iad.example'
export OCI_KAFKA_SASL_PASSWORD='your_token_here'
export OCI_CONFIG_PROFILE='DEFAULT'
```

Then run:

```bash
python3 get-stream-gap.py
python3 get-stream-gap-metrics.py
```

## Notes

- The OCI config profile should use the same region as the target stream.
- `get-stream-gap-metrics.py` uses the OCI Monitoring telemetry ingestion endpoint for the region in the OCI config profile.
- The original metric is based on exact offset lag, not an estimated "messages behind" value.
