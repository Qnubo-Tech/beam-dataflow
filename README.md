# Beam Dataflow Pipelines

This repository contains a set of Apache Beam pipelines examples with advance features. Each one of the examples
provides new concepts over the basic functionality of Apache Beam. These pipelines have been designed to run
in Google's Dataflow service because majority of the features are related to Google Cloud ecosystem, such as Spanner or
BigQuery.

## Examples

All pipelines described here focus on batch processing. The collection includes:

1. [`SideInputSchema`](https://github.com/Qnubo-Tech/beam-dataflow/blob/master/src/main/java/pipelines/SideInputSchema.java)
is a simple dummy pipeline that demonstrates how to use [Side Inputs](https://beam.apache.org/documentation/patterns/side-inputs/)
from an external file and introduces a custom `Record` class to process inside a PCollection.

2. [`SpannerIngester`](https://github.com/Qnubo-Tech/beam-dataflow/blob/master/src/main/java/pipelines/SpannerIngester.java)
is a pipeline that writes an external file into Spanner. The pipeline can be reused from one table to another thanks to the
custom `Record`. It writes into Spanner using the [SpannerIO](https://beam.apache.org/releases/javadoc/2.19.0/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.html)
class and shows how to handle the [Mutations](https://www.javadoc.io/doc/com.google.cloud/google-cloud-spanner/latest/com/google/cloud/spanner/Mutation.html).