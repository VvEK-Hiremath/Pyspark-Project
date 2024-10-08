#!/bin/bash

# Function to display usage
usage() {
    echo "Usage: $0 <environment>"
    echo "Environments: dev | test | prod"
    exit 1
}

# Check if the environment argument is provided
if [ $# -ne 1 ]; then
    usage
fi

# Set environment variables based on the argument
case $1 in
    dev)
        export SPARK_HOME="/path/to/dev/spark"
        export PYSPARK_PYTHON="python3"
        JOB_NAME="my_dev_job"
        ;;
    test)
        export SPARK_HOME="/path/to/test/spark"
        export PYSPARK_PYTHON="python3"
        JOB_NAME="my_test_job"
        ;;
    prod)
        export SPARK_HOME="/path/to/prod/spark"
        export PYSPARK_PYTHON="python3"
        JOB_NAME="my_prod_job"
        ;;
    *)
        usage
        ;;
esac

# Additional configurations
export PYSPARK_DRIVER_MEMORY="2g"
export PYSPARK_EXECUTOR_MEMORY="2g"
export PYSPARK_SUBMIT_ARGS="--master yarn --deploy-mode cluster"

# Print the configuration
echo "Running in $1 environment"
echo "Spark Home: $SPARK_HOME"
echo "Python: $PYSPARK_PYTHON"
echo "Job Name: $JOB_NAME"
echo "Driver Memory: $PYSPARK_DRIVER_MEMORY"
echo "Executor Memory: $PYSPARK_EXECUTOR_MEMORY"