ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8


FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

# WORKDIR /project
ARG PYSPARK_VERSION=3.2.0
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}
# RUN pip --no-cache-dir install pyspark
# COPY ./requirements.txt ./
# RUN pip install --no-cache-dir -r requirements.txt

COPY . .
# COPY ./src ./src

CMD python src/main.py