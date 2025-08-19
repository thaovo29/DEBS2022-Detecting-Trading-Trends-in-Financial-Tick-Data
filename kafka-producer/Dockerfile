
# Set the base image 
FROM python:3.10-alpine3.21

LABEL maintainer="Thaovo"

# dont write pyc files
ENV PYTHONDONTWRITEBYTECODE 1
# dont buffer to stdout/stderr
ENV PYTHONUNBUFFERED 1


# Make a directory for our app
WORKDIR /producer


# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY producer.py .
COPY seed.sh /producer/seed.sh
RUN chmod +x /producer/seed.sh

# keep a pristine copy of the dataset inside the image as seed source
# (the named volume will mount at /data and hide container FS there,
# so we store seed files under /seed)
COPY /seed/dataset/ /seed/dataset/

ENTRYPOINT ["/producer/seed.sh"]
CMD ["python", "producer.py"]