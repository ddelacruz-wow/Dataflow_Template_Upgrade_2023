FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY requirements.txt .
COPY pipeline_code.py .

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline_code.py"

RUN apt-get update

RUN apt-get install -y libffi7 libffi-dev

RUN pip install --upgrade pip

RUN pip install apache-beam[gcp]==2.38.0

RUN pip install -U -r ./requirements.txt