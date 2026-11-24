ARG DBT_VERSION=1.0.0
FROM fishtownanalytics/dbt:${DBT_VERSION}

ENV DBT_PROFILES_DIR=.

# Install utils
RUN printf '%s\n' \
    'deb https://archive.debian.org/debian bullseye main' \
    'deb https://archive.debian.org/debian-security bullseye-security main' \
    > /etc/apt/sources.list \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
    ca-certificates curl wget gpg unzip \
    && rm -rf /var/lib/apt/lists/*


RUN set -ex \
    && python -m pip install setuptools \
    && python -m pip install dbt-clickhouse==1.4.0 dbt-core==1.4.0 numpy

# Install yc CLI
RUN curl https://storage.yandexcloud.net/yandexcloud-yc/install.sh | \
    bash -s -- -a

# Install Terraform
ARG TERRAFORM_VERSION=1.4.6
RUN curl -sL https://hashicorp-releases.yandexcloud.net/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip -o terraform.zip \
    && unzip terraform.zip \
    && install -o root -g root -m 0755 terraform /usr/local/bin/terraform \
    && rm -rf terraform terraform.zip

ENTRYPOINT [ "tail", "-f", "/dev/null" ]