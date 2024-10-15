# Base build
# FROM python:3.10-alpine as base
# RUN apk add --update --virtual .build-deps \
#     build-base \
#     postgresql-dev \
#     python3-dev \
#     libpq

# COPY requirements.txt /app/requirements.txt
# RUN pip install -r /app/requirements.txt

# # Now multistage build
# FROM python:3.10-alpine
# RUN apk add libpq
# COPY --from=base /usr/local/lib/python3.10/site-packages/ /usr/local/lib/python3.10/site-packages/
# COPY --from=base /usr/local/bin/ /usr/local/bin/
# COPY . /app
# ENV PYTHONUNBUFFERED 1

FROM python:3.9-slim-buster

WORKDIR /app
COPY requirements.txt .
#RUN apt update -y && apt install default-libmysqlclient-dev libpq-dev python-dev build-essential -y
#RUN pip install --upgrade pip
#RUN pip install psycopg2-binary gunicorn
RUN pip install -r requirements.txt
COPY . .

EXPOSE 8000
CMD ["gunicorn", "python_dcim.wsgi:application", "--bind", "0.0.0.0:8000"]