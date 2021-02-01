# Spark Hackathon Setup

This document contains a few pointers and some high level information on how the Spark hackathon was setup.

## Data

The data consists of 2 parts:

1. The log data itself. This was generated using the GDD [Apache Log Generator](https://github.com/godatadriven/apacheLogGenerator)
2. The device type mapping. This was created with the `categorize_user_agents.py` script in this directory.

The data is available on the academy-data GCS bucket. If that data disappears for some reason the pointers above should allow easy re-generation of the data.

During the hackathon it is recommended for the students to download the data to their local machine.

### Device Type Mapping

We filter out devices that are not one of: `['desktop', 'smartphone', 'tablet', '']`. We leave in the `''` on purpose to add some complexity to the join.

## Solutions

A basic solution is provided in a notebook. This solution is not complete; the adding of tests etc. is relatively trivial.
