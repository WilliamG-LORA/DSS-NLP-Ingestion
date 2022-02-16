FROM python
RUN pip install redis

# TODO: fix path
COPY ./worker.py /worker.py 
COPY ./rediswq.py /rediswq.py

CMD  python worker.py