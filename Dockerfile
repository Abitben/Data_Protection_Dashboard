FROM quay.io/astronomer/astro-runtime:9.1.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate

RUN python -m venv custom_python && source custom_python/bin/activate && \
    pip install --no-cache-dir -r custom_python-requirements.txt && deactivate