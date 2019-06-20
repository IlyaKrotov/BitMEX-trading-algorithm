FROM python:3.6.3

RUN apt-get install git
WORKDIR /
RUN git clone https://github.com/BitMEX/api-connectors /usr/bitmex-ws
WORKDIR /usr/bitmex-ws/official-ws/python
RUN python setup.py install

WORKDIR /usr/bitmextracker
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p logs
WORKDIR /usr/bitmextracker/logs
CMD [ "python", "../BitmexTracker.py" ]
