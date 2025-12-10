# Start from the official Airflow image
FROM apache/airflow:3.1.3

# Switch to root to run system-level commands
USER root

# Install Java Development Kit (JDK)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Switch back to airflow user for normal operations
USER airflow

# Copy requirements.txt from local folder into the image
COPY requirements.txt /opt/airflow/requirements.txt

# Install the dependencies from requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt