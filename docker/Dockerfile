# =============================================
# Multi-Database Transform Application Dockerfile
# =============================================
# This Dockerfile creates a containerized environment for the .NET Spark application
# with all necessary dependencies for multi-database ETL processing

# Use official .NET 6.0 runtime as base image
FROM mcr.microsoft.com/dotnet/runtime:6.0

# Set environment variables
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3.3.4
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    wget \
    curl \
    unzip \
    python3 \
    python3-pip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Download and install Apache Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Download and install Hadoop
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz \
    && mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# Download Microsoft.Spark.Worker
RUN wget -q https://github.com/dotnet/spark/releases/download/v3.0.0/Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-3.0.0.tar.gz \
    && tar -xzf Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-3.0.0.tar.gz \
    && mv Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-3.0.0 Microsoft.Spark.Worker \
    && rm Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-3.0.0.tar.gz

# Create application directory
WORKDIR /app

# Copy project files
COPY *.csproj ./
COPY *.cs ./
COPY *.json ./
COPY *.sql ./
COPY *.sh ./

# Create jars directory and copy JDBC drivers
RUN mkdir -p jars

# Download JDBC drivers
RUN cd jars && \
    wget -q https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.4.1.jre8/mssql-jdbc-9.4.1.jre8.jar && \
    wget -q https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# Note: Oracle JDBC driver requires license agreement, so it's not downloaded automatically
# Users need to manually add ojdbc8.jar to the jars/ directory

# Restore .NET dependencies
RUN dotnet restore

# Build the application
RUN dotnet build -c Release -o out

# Create output directory
RUN mkdir -p output

# Create logs directory
RUN mkdir -p logs

# Copy Spark configuration
COPY spark-defaults.conf ${SPARK_HOME}/conf/

# Set permissions
RUN chmod +x *.sh

# Expose ports (if needed for Spark UI)
EXPOSE 4040 7077 8080 8081

# Set default command
CMD ["dotnet", "out/ETL.Scalable.dll"]
