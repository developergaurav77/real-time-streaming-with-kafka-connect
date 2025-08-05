FROM jupyter/pyspark-notebook:latest

# 1. Copy requirements file into the image
COPY requirements.txt /tmp/requirements.txt

# 2. Install everything in it
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
 && rm -f /tmp/requirements.txt || true
