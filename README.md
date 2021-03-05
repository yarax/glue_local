# AWS Glue mock library

*Important: the library is in development and not stable yet*

## Introducation

The goal is to be able to run Glue code locally, skipping all the Cloud-specific functions, working with local mocked files and testing only pyspark-transformations.
In the best case you have the same code, which can be run locally with this library and in the worst case you have to slightly adjust your code, but it's still remaining backward compatible with cloud.

The library is based on official AWS Glue repository https://github.com/awslabs/aws-glue-libs and adapted for Python 3

## Installation, preparation and running

* Install python3
* Install spark
* * For spark 2.4 you would need Java 8
* * For spark 3 - Java 11
* Install virtualenv: `pip3 install virtualenv`
* create an env with python 3.7 (>=3.8 is not yet supported by pyspark) `virtualenv -p /usr/local/opt/python@3.7/bin/python3.7 env`
* `source ./env/bin/activate`
* install pyspark `pip3 install pyspark`. Make sure, that your pyspark version matches with your global Spark installation
* `git clone https://github.com/yarax/glue_local.git awsglue` in your ETL scripts directory
* `python ./your_glue_script`

## How to map your cloud data locations with local ones

There are 2 possible ways to define your source data frame in Glue: `create_dynamic_frame_from_catalog` and `create_dynamic_frame_from_options`.

* glue_context.create_dynamic_frame_from_catalog or glue_context.create_dynamic_frame.from_catalog - in this case your table is the file name inside of `./tests/source_data` directory next to your script. The database name will be skipped.
* glue_context.create_dynamic_frame_from_options - in this case you have to provide an additional key for your `connection_options` parameter - `local_path: ./tests/source_data/my_data_file` without extention (it will be detected automatically)

If you want to customise your source files location, you can use GLUE_SOURCE_PATH environment variable
os.getcwd() "tests" source_data

## Restrictions

Currently only JSON and Parquet format are supported
Currently only S3 data source is supported

Using of Glue transform methods https://docs.aws.amazon.com/glue/latest/dg/built-in-transforms.html is not recommended and can work unexpectedly. Instead you can use Spark transformations https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations direct on the dataframe