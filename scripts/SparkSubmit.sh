#!/bin/bash

# =============================================
# Spark Submit Script for Multi-Database Transform
# =============================================
# This script submits the .NET Spark application to a Spark cluster
# with optimized configurations for large-scale data processing

# Configuration Variables
SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
JAVA_HOME=${JAVA_HOME:-"/usr/lib/jvm/java-8-openjdk"}

# Application Configuration
APP_NAME="MultiDBTransform"
MASTER="yarn"
DEPLOY_MODE="cluster"
DRIVER_MEMORY="4g"
DRIVER_CORES="2"
EXECUTOR_MEMORY="8g"
EXECUTOR_CORES="4"
NUM_EXECUTORS="10"
MAX_EXECUTORS="20"

# JAR Files
SPARK_WORKER_JAR="Microsoft.Spark.Worker-3.0.0.jar"
APP_JAR="ETL.Scalable.dll"

# JDBC Driver JARs
JDBC_JARS=""
if [ -d "jars" ]; then
    for jar in jars/*.jar; do
        if [ -f "$jar" ]; then
            if [ -z "$JDBC_JARS" ]; then
                JDBC_JARS="$jar"
            else
                JDBC_JARS="$JDBC_JARS,$jar"
            fi
        fi
    done
fi

# Spark Configuration
SPARK_CONF=""
SPARK_CONF="$SPARK_CONF --conf spark.sql.adaptive.enabled=true"
SPARK_CONF="$SPARK_CONF --conf spark.sql.adaptive.coalescePartitions.enabled=true"
SPARK_CONF="$SPARK_CONF --conf spark.sql.adaptive.skewJoin.enabled=true"
SPARK_CONF="$SPARK_CONF --conf spark.sql.adaptive.localShuffleReader.enabled=true"
SPARK_CONF="$SPARK_CONF --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=134217728"
SPARK_CONF="$SPARK_CONF --conf spark.sql.files.maxPartitionBytes=134217728"
SPARK_CONF="$SPARK_CONF --conf spark.sql.files.openCostInBytes=4194304"
SPARK_CONF="$SPARK_CONF --conf spark.sql.broadcastTimeout=3600"
SPARK_CONF="$SPARK_CONF --conf spark.sql.autoBroadcastJoinThreshold=100485760"
SPARK_CONF="$SPARK_CONF --conf spark.sql.shuffle.partitions=200"
SPARK_CONF="$SPARK_CONF --conf spark.default.parallelism=200"
SPARK_CONF="$SPARK_CONF --conf spark.sql.execution.arrow.pyspark.enabled=true"
SPARK_CONF="$SPARK_CONF --conf spark.sql.execution.arrow.pyspark.fallback.enabled=true"
SPARK_CONF="$SPARK_CONF --conf spark.executor.extraJavaOptions=-XX:+UseG1GC"
SPARK_CONF="$SPARK_CONF --conf spark.driver.extraJavaOptions=-XX:+UseG1GC"
SPARK_CONF="$SPARK_CONF --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
SPARK_CONF="$SPARK_CONF --conf spark.kryoserializer.buffer.max=1024m"
SPARK_CONF="$SPARK_CONF --conf spark.sql.execution.arrow.maxRecordsPerBatch=10000"
SPARK_CONF="$SPARK_CONF --conf spark.sql.execution.arrow.pyspark.enabled=true"
SPARK_CONF="$SPARK_CONF --conf spark.sql.execution.arrow.pyspark.fallback.enabled=true"

# Performance Monitoring
SPARK_CONF="$SPARK_CONF --conf spark.eventLog.enabled=true"
SPARK_CONF="$SPARK_CONF --conf spark.eventLog.dir=hdfs:///spark-events"
SPARK_CONF="$SPARK_CONF --conf spark.sql.statistics.size.autoUpdate.enabled=true"
SPARK_CONF="$SPARK_CONF --conf spark.sql.statistics.histogram.enabled=true"

# Memory and GC Configuration
SPARK_CONF="$SPARK_CONF --conf spark.executor.memoryOverhead=2g"
SPARK_CONF="$SPARK_CONF --conf spark.driver.memoryOverhead=1g"
SPARK_CONF="$SPARK_CONF --conf spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UnlockExperimentalVMOptions -XX:+UseStringDeduplication"
SPARK_CONF="$SPARK_CONF --conf spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Network Configuration
SPARK_CONF="$SPARK_CONF --conf spark.network.timeout=800s"
SPARK_CONF="$SPARK_CONF --conf spark.executor.heartbeatInterval=60s"
SPARK_CONF="$SPARK_CONF --conf spark.rpc.askTimeout=600s"
SPARK_CONF="$SPARK_CONF --conf spark.rpc.lookupTimeout=600s"

# Dynamic Allocation
SPARK_CONF="$SPARK_CONF --conf spark.dynamicAllocation.enabled=true"
SPARK_CONF="$SPARK_CONF --conf spark.dynamicAllocation.minExecutors=5"
SPARK_CONF="$SPARK_CONF --conf spark.dynamicAllocation.maxExecutors=20"
SPARK_CONF="$SPARK_CONF --conf spark.dynamicAllocation.initialExecutors=10"
SPARK_CONF="$SPARK_CONF --conf spark.dynamicAllocation.executorIdleTimeout=60s"
SPARK_CONF="$SPARK_CONF --conf spark.dynamicAllocation.schedulerBacklogTimeout=1s"

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help              Display this help message"
    echo "  -m, --master MASTER     Spark master URL (default: yarn)"
    echo "  -d, --deploy-mode MODE  Deploy mode: client or cluster (default: cluster)"
    echo "  -e, --executors NUM     Number of executors (default: 10)"
    echo "  -em, --executor-memory MEM  Executor memory (default: 8g)"
    echo "  -ec, --executor-cores CORES  Executor cores (default: 4)"
    echo "  -dm, --driver-memory MEM     Driver memory (default: 4g)"
    echo "  -dc, --driver-cores CORES    Driver cores (default: 2)"
    echo "  -j, --jars JARS         Additional JAR files (comma-separated)"
    echo "  -c, --config KEY=VALUE  Additional Spark configuration"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Run with default settings"
    echo "  $0 -m local[*] -d client              # Run locally in client mode"
    echo "  $0 -e 20 -em 16g -ec 8                # Run with 20 executors, 16GB memory, 8 cores"
    echo "  $0 -c spark.sql.shuffle.partitions=400 # Override shuffle partitions"
    echo ""
}

# Function to validate environment
validate_environment() {
    echo "Validating environment..."
    
    # Check if Spark is available
    if [ ! -d "$SPARK_HOME" ]; then
        echo "ERROR: SPARK_HOME not found at $SPARK_HOME"
        echo "Please set SPARK_HOME environment variable"
        exit 1
    fi
    
    # Check if Java is available
    if [ ! -d "$JAVA_HOME" ]; then
        echo "ERROR: JAVA_HOME not found at $JAVA_HOME"
        echo "Please set JAVA_HOME environment variable"
        exit 1
    fi
    
    # Check if application files exist
    if [ ! -f "$APP_JAR" ]; then
        echo "ERROR: Application JAR not found: $APP_JAR"
        echo "Please build the application first: dotnet build"
        exit 1
    fi
    
    if [ ! -f "$SPARK_WORKER_JAR" ]; then
        echo "ERROR: Spark Worker JAR not found: $SPARK_WORKER_JAR"
        echo "Please download Microsoft.Spark.Worker"
        exit 1
    fi
    
    echo "Environment validation passed."
}

# Function to build spark-submit command
build_spark_submit_command() {
    local cmd="$SPARK_HOME/bin/spark-submit"
    
    # Basic configuration
    cmd="$cmd --class org.apache.spark.deploy.dotnet.DotnetRunner"
    cmd="$cmd --name \"$APP_NAME\""
    cmd="$cmd --master $MASTER"
    cmd="$cmd --deploy-mode $DEPLOY_MODE"
    cmd="$cmd --driver-memory $DRIVER_MEMORY"
    cmd="$cmd --driver-cores $DRIVER_CORES"
    cmd="$cmd --executor-memory $EXECUTOR_MEMORY"
    cmd="$cmd --executor-cores $EXECUTOR_CORES"
    cmd="$cmd --num-executors $NUM_EXECUTORS"
    cmd="$cmd --conf spark.dynamicAllocation.maxExecutors=$MAX_EXECUTORS"
    
    # Add JDBC JARs if available
    if [ -n "$JDBC_JARS" ]; then
        cmd="$cmd --jars $JDBC_JARS"
    fi
    
    # Add additional JARs if specified
    if [ -n "$ADDITIONAL_JARS" ]; then
        cmd="$cmd --jars $ADDITIONAL_JARS"
    fi
    
    # Add Spark configurations
    cmd="$cmd $SPARK_CONF"
    
    # Add additional configurations
    if [ -n "$ADDITIONAL_CONFIGS" ]; then
        for config in $ADDITIONAL_CONFIGS; do
            cmd="$cmd --conf $config"
        done
    fi
    
    # Add the Spark Worker JAR and application
    cmd="$cmd $SPARK_WORKER_JAR"
    cmd="$cmd dotnet $APP_JAR"
    
    echo "$cmd"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -m|--master)
            MASTER="$2"
            shift 2
            ;;
        -d|--deploy-mode)
            DEPLOY_MODE="$2"
            shift 2
            ;;
        -e|--executors)
            NUM_EXECUTORS="$2"
            shift 2
            ;;
        -em|--executor-memory)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        -ec|--executor-cores)
            EXECUTOR_CORES="$2"
            shift 2
            ;;
        -dm|--driver-memory)
            DRIVER_MEMORY="$2"
            shift 2
            ;;
        -dc|--driver-cores)
            DRIVER_CORES="$2"
            shift 2
            ;;
        -j|--jars)
            ADDITIONAL_JARS="$2"
            shift 2
            ;;
        -c|--config)
            ADDITIONAL_CONFIGS="$ADDITIONAL_CONFIGS $2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Main execution
echo "============================================="
echo "Multi-Database Transform Spark Submit Script"
echo "============================================="
echo ""

# Validate environment
validate_environment

echo ""
echo "Configuration:"
echo "  Master: $MASTER"
echo "  Deploy Mode: $DEPLOY_MODE"
echo "  Executors: $NUM_EXECUTORS"
echo "  Executor Memory: $EXECUTOR_MEMORY"
echo "  Executor Cores: $EXECUTOR_CORES"
echo "  Driver Memory: $DRIVER_MEMORY"
echo "  Driver Cores: $DRIVER_CORES"
echo "  JDBC JARs: $JDBC_JARS"
echo ""

# Build and execute spark-submit command
SPARK_SUBMIT_CMD=$(build_spark_submit_command)

echo "Executing Spark Submit Command:"
echo "$SPARK_SUBMIT_CMD"
echo ""

# Execute the command
eval $SPARK_SUBMIT_CMD

# Check exit status
if [ $? -eq 0 ]; then
    echo ""
    echo "============================================="
    echo "Spark application completed successfully!"
    echo "============================================="
else
    echo ""
    echo "============================================="
    echo "Spark application failed!"
    echo "============================================="
    exit 1
fi
