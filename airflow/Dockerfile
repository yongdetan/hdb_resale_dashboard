FROM apache/airflow:2.2.3-python3.8
ENV AIRFLOW_HOME=/opt/airflow
USER root

# Install openjdk (for pyspark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean

# Install google cloud sdk https://airflow.apache.org/docs/docker-stack/recipes.html
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

# Install the various libraries that are needed for the pipeline
USER $AIRFLOW_UID
RUN pip install --upgrade pip

COPY requirements.txt /opt/airflow
WORKDIR /opt/airflow
RUN pip install -r requirements.txt

WORKDIR $AIRFLOW_HOME

