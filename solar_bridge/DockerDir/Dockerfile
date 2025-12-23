# Use a lightweight Python base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Install dependencies
# We run this in one line to keep the image layer small
RUN pip install --no-cache-dir \
    hoymiles-modbus \
    paho-mqtt \
    python-dotenv  

# Copy the script into the container
COPY solar_bridge.py .

# Command to run when the container starts
CMD ["python", "-u", "solar_bridge.py"]
