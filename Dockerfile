FROM python:3.7

RUN apt -y update && apt -y install git
RUN pip install git+https://github.com/jhuapl-boss/ingest-client

CMD ["boss-ingest", "-h"]
