# Use the official Ubuntu image as the base image
FROM ubuntu:20.04

# Set the environment variables for the image
ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Install required packages
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    postgresql-client \
    postgresql-server-dev-all \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip3 install --upgrade pip

# Copy your application code into the container
COPY . /stock_analysis

# Set the working directory to /app
WORKDIR /stock_analysis

# Install the dependencies for your application
RUN pip3 install -r requirements.txt


