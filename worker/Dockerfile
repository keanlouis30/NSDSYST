FROM python:3.11-slim

WORKDIR /app

COPY worker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY worker/main.py ./main.py

CMD ["python", "main.py"]
