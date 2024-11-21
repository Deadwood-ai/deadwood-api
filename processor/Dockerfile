# Use NVIDIA CUDA base image with Ubuntu 22.04
FROM nvidia/cuda:12.3.1-runtime-ubuntu22.04 as cuda_base

# Use official GDAL image for Ubuntu
FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

# Copy CUDA libraries and runtime from the CUDA base image
COPY --from=cuda_base /usr/local/cuda/lib64 /usr/local/cuda/lib64
COPY --from=cuda_base /usr/local/cuda/include /usr/local/cuda/include

# Set CUDA environment variables
ENV PATH=/usr/local/cuda/bin:$PATH \
    LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH

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

# Install PyTorch with CUDA support
RUN pip3 install --no-cache-dir \
    torch \
    torchvision

# Set up the application
WORKDIR /app

# Copy and install requirements
COPY processor/requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

# Copy application files
COPY ./processor/src /app/processor/src
COPY ./processor/src/deadwood_segmentation/models/model.safetensors /app/processor/src/deadwood_segmentation/models/model.safetensors
COPY ./shared /app/shared

# Run the processor script
CMD ["python3", "-m", "processor.src.processor"]
