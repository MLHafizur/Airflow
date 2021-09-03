FROM python:3.8-slim

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install -r /tmp/requirements.txt

COPY fatch_rating.py /tmp/fatch_rating.py
RUN chmod +x /tmp/fatch_rating.py

ENV PATH="/usr:${PATH}"