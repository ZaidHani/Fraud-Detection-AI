FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install -r /app/requirements.txt

# copy the rest of the application code
COPY . .

# set the command to run the app
EXPOSE 5500