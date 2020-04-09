# Beam Dataflow Pipelines

This repository contains a set of Apache Beam pipelines examples with advance features. Each one of the examples
provides new concepts over the basic functionality of Apache Beam. These pipelines have been designed to run
in Google's Dataflow service because majority of the features are related to Google Cloud ecosystem, such as Spanner or
BigQuery.

## Examples

All pipelines described here focus on batch processing. The collection includes:

1. [`SideInputSchema`](https://github.com/NonoMalpi/beam-dataflow/blob/master/src/main/java/pipelines/SideInputSchema.java)
is a simple dummy pipeline that demonstrates how to use [Side Inputs](https://beam.apache.org/documentation/patterns/side-inputs/)
from an external file and introduces a custom `Record` class to process inside a PCollection.