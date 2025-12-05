# Start from the official Airflow image
FROM apache/airflow:3.1.3

# Switch to roor to run system-level commands
# USER airflow

# Copy requirements.txt from local folder into the image
COPY requirements.txt /opt/airflow/requirements.txt

# Install the dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt