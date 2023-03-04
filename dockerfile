FROM python:3.11.1-slim

WORKDIR /app
RUN apt-get update && apt-get install -y iptables

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080

EXPOSE $PORT

CMD ["python3","server.py"]