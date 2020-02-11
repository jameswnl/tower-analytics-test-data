FROM centos/python-36-centos7

RUN pip install pipenv

COPY . /app

WORKDIR /app

RUN pipenv install --deploy --ignore-pipfile

EXPOSE 8000

CMD ["pipenv", "run", "uvicorn", "api.main:app"]
