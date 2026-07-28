FROM python:3.11-slim

WORKDIR /app

COPY forwarder/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY forwarder/forwarder.py ./forwarder.py

ENTRYPOINT ["python", "forwarder.py"]
