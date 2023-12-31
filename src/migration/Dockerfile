# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG TAG=latest
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:${TAG}

ARG WORKDIR=/dataflow/template
ARG APP_SOURCE=app/

RUN mkdir -p ${WORKDIR}
RUN mkdir -p ${WORKDIR}/${APP_SOURCE}

WORKDIR ${WORKDIR}

COPY ./${APP_SOURCE} ./${APP_SOURCE}

# copy __init__ for setup.py
COPY ./__init__.py .
COPY ./setup.py .
COPY ./requirements.txt .

ENV PYTHONPATH ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/${APP_SOURCE}/pipeline_template.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"

RUN apt-get update -y \
    && apt-get install python3-dev default-libmysqlclient-dev build-essential pkg-config -y

RUN export MYSQLCLIENT_CFLAGS="-I/usr/include/mysql" \
    && export MYSQLCLIENT_LDFLAGS="-L/usr/lib/x86_64-linux-gnu -lmysqlclient"

# Upgrade pip and install the requirements.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install mysqlclient==2.2.1 \
    && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
    # Download the requirements to speed up launching the Dataflow job.
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

RUN python ${FLEX_TEMPLATE_PYTHON_SETUP_FILE} install

## Since we already downloaded all the dependencies, there's no need to rebuild everything.
ENV PIP_NO_DEPS=True