FROM python:3.7

RUN apt -y update
RUN apt -y install git
RUN git clone https://github.com/jhuapl-boss/ingest-client
RUN pip install ./ingest-client

CMD ["boss-ingest", "-h"]
