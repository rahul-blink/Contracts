# Self-contained image for special-quetzal.apps.blinkit.in, built from the Mac
# with `chef skaffold up`. fees.duckdb (~430 MB) is copied before the code so a
# code-only change reuses the cached DB layer and pushes only a few KB. The
# saved lists (opt-ins, exclusions, KAM addenda) are NOT in the image: they live
# on the fee-insights-state PVC mounted at /state and survive every deploy.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fees.duckdb .
COPY static ./static
COPY app.py build_db.py load_contracts.py ./

ENV PORT=8000
EXPOSE 8000
CMD ["python", "app.py"]
