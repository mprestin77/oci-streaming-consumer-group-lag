#!/usr/bin/env python3

import os
import certifi
import oci
from confluent_kafka import Consumer, TopicPartition


def build_kafka_consumer():
      return Consumer({
          "bootstrap.servers": os.environ["OCI_KAFKA_BOOTSTRAP_SERVERS"],
          "security.protocol": "SASL_SSL",
          "sasl.mechanism": "PLAIN",
          "sasl.username": os.environ["OCI_KAFKA_SASL_USERNAME"],
          "sasl.password": os.environ["OCI_KAFKA_SASL_PASSWORD"],
          "ssl.ca.location": certifi.where(),
          "group.id": "lag-inspector",
          "enable.auto.commit": False,
      })


def main():
      stream_id = os.environ["OCI_STREAM_ID"]
      group_name = os.environ["OCI_GROUP_NAME"]
      profile = os.getenv("OCI_CONFIG_PROFILE", "DEFAULT")

      config = oci.config.from_file(profile_name=profile)

      # OCI admin call: get stream metadata.
      admin_client = oci.streaming.StreamAdminClient(config)
      stream = admin_client.get_stream(stream_id).data

      topic = stream.name
      messages_endpoint = stream.messages_endpoint

      # OCI data-plane call: get consumer group state.
      stream_client = oci.streaming.StreamClient(
          config,
          service_endpoint=messages_endpoint,
      )
      group = stream_client.get_group(stream_id, group_name).data

      kafka = build_kafka_consumer()

      try:
          total_raw_offset_gap = 0

          print(f"stream={topic} group={group_name}")
          print("partition committed_offset end_offset raw_offset_gap")

          for r in sorted(group.reservations, key=lambda x: int(x.partition)):
              partition = int(r.partition)
              committed = int(r.committed_offset)

              # Kafka high watermark = last message offset + 1
              low, high = kafka.get_watermark_offsets(
                  TopicPartition(topic, partition),
                  timeout=10,
              )
              end_offset = high

              # Direct numeric difference between latest and committed.
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

      finally:
          kafka.close()


if __name__ == "__main__":
      main()
