FROM mcr.microsoft.com/playwright/python:v1.46.0-jammy

WORKDIR /app

# Copy requirements from the build context's data/ directory
COPY data/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application scripts into the image
COPY data/scrape_waltermart_dasma.py data/viewer_streamlit.py ./

# Ensure a directory for persisted results exists (mounted by docker-compose)
RUN mkdir -p /app/data

# Make Python output unbuffered (helps with container logs)
ENV PYTHONUNBUFFERED=1

CMD ["python", "-c", "print('Specify a command: scrape | viewer')"]