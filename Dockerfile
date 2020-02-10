FROM python:3.8.0

COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD loonfilter/benchmarks.py
