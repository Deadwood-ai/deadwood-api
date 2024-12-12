# Start with GDAL image
FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

# Install Python and development tools
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    python3-venv \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set environment variables for GDAL
RUN echo "GDAL_VERSION=$(gdal-config --version)" >> /etc/environment
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal

# Install PyTorch - it will automatically detect GPU availability
RUN pip3 install --no-cache-dir \
    torch \
    torchvision

# Set up the application
WORKDIR /app
RUN mkdir -p /data

# Copy and install requirements
COPY processor/requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

# Copy application files
COPY ./processor/src /app/processor/src
COPY ./shared /app/shared

# Copy model file
COPY ./processor/src/deadwood_segmentation/models/model.safetensors /app/processor/src/deadwood_segmentation/models/model.safetensors

# Run the processor script
CMD ["python3", "-m", "processor.src.processor"]
