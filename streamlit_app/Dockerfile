# Use the official Python image from DockerHub as the base image
FROM python:3.9-slim-buster

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Make sure the system dependencies are installed
RUN apt-get update && apt-get install -y libxml2-dev libxslt-dev python3-dev

# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Specify the default command to run the Streamlit app
CMD ["streamlit", "run", "🏠_Home.py"]
