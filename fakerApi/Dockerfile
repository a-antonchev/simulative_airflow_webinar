FROM python:3.12-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

EXPOSE 8000

COPY ./ ./

CMD ["uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "8000"]
