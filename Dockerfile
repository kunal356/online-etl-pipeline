FROM bitnami/spark:3.5.0

USER root
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install pyspark findspark

WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies from requirements.txt
RUN pip3 install -r requirements.txt

WORKDIR /app
CMD ["python3", "-m", "unittest", "discover", "-s", "tests", "-v"]
