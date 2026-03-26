FROM quay.io/fedora/python-313-minimal:latest

ENV CHRONICLER_CONFIG="/config/config.yaml"

USER 0
WORKDIR /chronicler
COPY . /chronicler

RUN python3 -m pip install .

ENTRYPOINT ['python3', '-m', 'chronicler.run_postprocessing']
