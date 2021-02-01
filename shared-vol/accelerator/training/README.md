Spark Streaming
===============

Notebooks and exercises for the Spark (Batch) training. This material is aimed at Data Engineers,
and is intended to be a day-long module, leading into the Spark (Batch) hackathon.

Versioning
----------

This version of the material covers Spark 2.4.5, and the examples work with Python 3.

Slide Generation & Presentation
-------------------------------

First, setup a python virtual environment:

    % mkvirtualenv -p python3.7 spark-training
    % pip install -r requirements.txt

Some of the slides process data that needs to be downloaded:

    % ./data/collect-data.sh

The slides include live code to execute, so present from within Jupyter. Some code examples use Arrow,
so you might need to set the following environment variable for compatibility with Spark:

    % export ARROW_PRE_0_15_IPC_FORMAT=1

_Note: The transitions that RISE uses don't work correctly in Safari. Present from Chrome instead._
