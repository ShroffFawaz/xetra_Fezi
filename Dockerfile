FROM python:3.9-slim-buster

# do not cache python packages
ENV PIP_NO_CACHE_DIR=yes

#Keeps python form generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

#set PYTHONPATH
ENV PYTHONPATH ="${PYTHONPATH} :/code/"

#Initializing new working directory
COPY xetra ./xetra
COPY Pipfile ./Pipfile
COPY Pipfile.lock ./Pipfile.lock
COPY run.py ./run.py

RUN pip install pipenv 
RUN pipenv install --ignore-pipfile --system
