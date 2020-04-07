FROM python:3.8-slim-buster as baseStage
MAINTAINER rey <sebastien.rey-coyrehourcq@univ-rouen.fr>

RUN apt-get update && apt-get install -y wget gnupg2 ca-certificates curl bash sudo gosu

ARG GID
ARG UID

ENV TZ=UTC

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN groupadd -g $GID flask && useradd -m -s /bin/sh -g flask -u $UID flask

COPY . /home/flask/covid
WORKDIR /home/flask/covid/

# install dependencies
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "wsgi:app"]

