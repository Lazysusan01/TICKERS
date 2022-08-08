# # syntax=docker/dockerfile:1
# FROM python:3.7.4-alpine3.10

# WORKDIR /app

# COPY . /app
# RUN  pip install -r requirements.txt

# EXPOSE 8000 

# CMD [ "python", '/app/main.py'] 

# ---------------------

# syntax=docker/dockerfile:1
FROM python:3.7.4-alpine3.10

WORKDIR C:\Users\nicom\OneDrive - Braemar Shipping Services plc\Documents\Python\Ticker Data Feeds\TICKERS PYTHON SCRIPT
RUN /bin/bash -c
# install app
COPY . /app
# install app dependencies
RUN pip install -r requirements.txt


# final configuration
EXPOSE 8000
CMD /app/main run --host 0.0.0.0 --port 8000