FROM apache/airflow:3.1.6

# Create modules directory
# we need to switch to root for system-level operations
USER root 
RUN mkdir -p /app/modules

USER airflow
# Copy requirements file
COPY requirements.txt /tmp/requirements.txt
# Install additional Python packages
RUN pip install -r /tmp/requirements.txt
# Switch back to airflow user for safety
# Ensures the container runs as the airflow user rather than root, following the principle of least privilege for security.
USER airflow