FROM centos/python-36-centos7

RUN pip install pipenv

COPY . /app

WORKDIR /app

RUN pipenv install --deploy --ignore-pipfile

EXPOSE 8000

CMD ["pipenv", "run", "sh", "/app/entrypoint", "gunicorn", "-w 2", "-b 0.0.0.0:8000", "-k uvicorn.workers.UvicornWorker", "api.main:app"]
