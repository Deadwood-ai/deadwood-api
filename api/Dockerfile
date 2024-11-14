# pull a Python 3.12 version
FROM python:3.12.1

# install GDAL and the matching pathon bindings
# TODO: THIS IS QUICK AND DIRTY. This is only necessary, because utils subpackage uses the type annotations
# of rasterio, which in turn needs GDAL to be present.
RUN apt-get update && apt-get install -y gdal-bin libgdal-dev && \
    pip install --upgrade pip && \
    pip install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')

# install everything
COPY api/requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    rm requirements.txt

# create a folder for the application
WORKDIR /app
COPY ./shared /app/shared
COPY ./api/src /app/api/src
COPY ./run.py /app/run.py



# set the working directory
WORKDIR /app

# run the application
#CMD ["uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
CMD ["python", "run.py", "server"]