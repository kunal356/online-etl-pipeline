FROM bitnami/spark:3.5.0

USER root
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install pyspark findspark

WORKDIR /app
CMD ["python3", "-m", "unittest", "discover", "-s", "tests", "-v"]
