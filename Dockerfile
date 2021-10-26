FROM python:3.8.0

COPY . .
RUN pip install --upgrade pip
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -
RUN export PATH="/root/.local/bin:$PATH"
RUN /root/.local/bin/poetry install

CMD larkspur/benchmarks.py
