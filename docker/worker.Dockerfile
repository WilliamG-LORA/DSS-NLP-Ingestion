# TODO shrink this image. It's like 1 GB..
FROM python:3.7

COPY src/requirements.txt /requirements.txt
RUN pip install -r requirements.txt

COPY src/ .

ENTRYPOINT [ "python" ]
