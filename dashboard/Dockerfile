FROM python:3.12-slim

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App + builder + pre-built analytics DB + static assets.
# NOTE: the raw parquet files in ../data are NOT copied — only the compact
# aggregated insights.duckdb (built by build_db.py) ships in the image. The
# builder is copied too so the "Manage Data" tab can rebuild from uploads.
COPY app.py .
COPY build_db.py .
COPY insights.duckdb .
COPY static ./static

ENV HOST=0.0.0.0 \
    PORT=8000
EXPOSE 8000

CMD ["python", "app.py"]
