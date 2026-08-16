ARG DBT_VERSION=1.0.0
FROM fishtownanalytics/dbt:${DBT_VERSION}

ENV DBT_PROFILES_DIR=.

# Install utils
RUN apt -y update \
    && apt -y upgrade \
    && apt -y install curl wget gpg unzip vim iputils-ping\
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

RUN mkdir -p /usr/local/share/ca-certificates/ && \
    wget "https://storage.yandexcloud.net/cloud-certs/RootCA.pem" \
         --output-document /usr/local/share/ca-certificates/Yandex_RootCA.crt && \
    wget "https://storage.yandexcloud.net/cloud-certs/IntermediateCA.pem" \
         --output-document /usr/local/share/ca-certificates/Yandex_IntermediateCA.crt && \
    chmod 644 /usr/local/share/ca-certificates/Yandex_RootCA.crt \
              /usr/local/share/ca-certificates/Yandex_IntermediateCA.crt && \
    update-ca-certificates 

    

ENTRYPOINT [ "tail", "-f", "/dev/null" ]
