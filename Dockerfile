FROM python:3.5

WORKDIR /azero-stock-data-sync

RUN pip install Flask
RUN pip install requests
RUN pip install pandas
RUN pip install pymongo
RUN apt-get update
RUN apt-get install wget
RUN apt-get install unzip
RUN pip install pandas_market_calendars
RUN wget http://interactivebrokers.github.io/downloads/twsapi_macunix.973.07.zip
RUN unzip twsapi_macunix.973.07.zip
RUN cd IBJts/source/pythonclient/ && python3 setup.py sdist
RUN cd IBJts/source/pythonclient/ && python3 setup.py bdist_wheel
RUN cd IBJts/source/pythonclient/ && python3 -m pip install --user --upgrade dist/ibapi-9.73.7-py3-none-any.whl

EXPOSE 5000

ENTRYPOINT ["python3", "app.py"]
