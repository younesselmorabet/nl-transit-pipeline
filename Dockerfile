# Start from a minimal, official Python image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy just the requirements file first (this lets Docker cache this step,
# so it doesn't reinstall everything every time you change your code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the actual code in
COPY ingest.py .

# Command that runs when the container starts
CMD ["python", "ingest.py"]