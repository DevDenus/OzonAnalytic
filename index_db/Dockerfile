FROM python:3.11-slim AS base

WORKDIR /index_db

RUN pip install --upgrade pip setuptools wheel

COPY . .

RUN python setup.py sdist bdist_wheel
RUN pip install dist/index_db-0.1-py3-none-any.whl
