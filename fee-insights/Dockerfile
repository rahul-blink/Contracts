# Layer on top of the image currently serving special-quetzal.apps.blinkit.in.
# fees.duckdb (429 MB, built Aug 4), echarts, the geojson and every Python
# dependency come from that image byte-for-byte; only the two files changed by
# the satellite fee exclusion list are replaced. The raw Parquet needed to
# rebuild fees.duckdb is not in this repo, so this is the only way to ship code
# changes without touching the data.
FROM 501578625374.dkr.ecr.ap-south-1.amazonaws.com/chef/blinkit-apps-saxena-rahul/fee-insights/fee-insights:dev-20260804-111006

WORKDIR /app
COPY app.py /app/app.py
COPY static/index.html /app/static/index.html

ENV PORT=8000
EXPOSE 8000
CMD ["python", "app.py"]
