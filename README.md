# ECCDynamicVideoTranscoder

## Overview

A cloud-native, distributed video transcoding pipeline leveraging Apache Spark on Jetstream2 and AWS services (S3, Lambda, DynamoDB, CloudFront + HLS). This project splits input videos into fixed-duration segments, processes them in parallel via Spark executors running FFmpeg, and reassembles them into HLS-compatible outputs, reducing end-to-end latency and improving scalability.

## Features

- Segment-based Parallel Transcoding: Split videos into 2-minute chunks using FFmpeg.

- Apache Spark on YARN: Distribute segment tasks across Jetstream2 worker nodes with dynamic executor allocation.

- AWS Integration:

  - S3 for storing input, intermediate segments, and final outputs.

  - Lambda for event-driven job triggering.

  - DynamoDB for metadata storage and fault-tolerant retries.

  - CloudFront + HLS for adaptive bitrate streaming.

- Job Locking via DynamoDB
