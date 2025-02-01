# Databricks notebook source
!pip install --upgrade pip
!pip install kafka-python
!pip install pyYaml
!pip install orjson
!pip install aiokafka 
!pip install --upgrade websockets
dbutils.library.restartPython()

"""
# !pip install cryptofeed[all] # add note about the cryptofeed install from image
git clone cryptofeed into workspace on dbx (https://github.com/bmoscon/cryptofeed/tree/master -> to clone repo )
replace the coinbase.py file with the new file in directory here as 'coinbase.py'
then need to rebuild the repo and install the .whl file to use the local repo. at the dbx cluster, build from wheel.
go to databricks > conmpute > libraries > install from wheel and select the created .whl file for cryptofeed 

!pip install setuptools wheel
!python setup.py bdist_wheel

relevant links;
https://github.com/bmoscon/cryptofeed/tree/master -> to clone repo 
https://github.com/kzk2000/deephaven-clickhouse -> contained example code for kafka & coinbase.py update
https://cloud.redpanda.com/clusters/ -> setup redpanda cluster for kafka usage
"""


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a 'trade' topic in redpanda after creating a cluster: 
# MAGIC - https://cloud.redpanda.com/clusters/cthki8qfdq8asdnsm9gg/topics

# COMMAND ----------

#### OPTIONAL
# # Store secrets securely using databricks-cli (RECOMMENDED)
# %pip install databricks-cli

# databricks configure --token
# atabricks secrets create-scope --scope kafka

# dbutils.secrets.put(scope="kafka", key="username", string_value="your-username")
# dbutils.secrets.put(scope="kafka", key="password", string_value="your-password")
# dbutils.secrets.put(scope="kafka", key="bootstrap_servers", string_value="your-bootstrap-servers")

# Placeholder values (BETTER TO USE THE DBX CLI SECRET KEY VALUES)
username = "<ENTER_YOUR_USERNAME>"
password = "<ENTER_YOUR_PASSWORD>"
bootstrap_servers = "<ENTER_YOUR_BOOTSTRAP_SERVERS>"


# COMMAND ----------

import asyncio
import logging
import sys
import os
from pathlib import Path
import nest_asyncio

# Apply nest_asyncio to handle nested loops
nest_asyncio.apply()

# Adjust the Python path if needed
sys.path.append(str(Path(os.getcwd().split("src")[0] + "src").absolute()))

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, L2_BOOK
from cryptofeed.exchanges import Coinbase, Bitstamp, Kraken
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import cryptofeed_tools as cft


def ensure_topics(bootstrap_servers, username, password, sasl_mechanism="SCRAM-SHA-256"):
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=username,
        sasl_plain_password=password,
    )

    topics_to_create = ["trade", "orderbooks"]
    existing_topics = admin.list_topics()

    new_topics = []
    for t in topics_to_create:
        if t not in existing_topics:
            new_topics.append(NewTopic(name=t, num_partitions=1, replication_factor=3))

    if new_topics:
        try:
            admin.create_topics(new_topics=new_topics)
            print("Created topics:", [nt.name for nt in new_topics])
        except TopicAlreadyExistsError:
            print("Some topics already exist.")
    else:
        print("All topics already exist")

    admin.close()


async def run_feed_handler():
    """
    Retrieve Kafka credentials from Databricks widgets if available.
    Otherwise, use placeholder values that must be replaced with actual credentials.
    """

    # # Placeholder values (BETTER TO USE THE DBX CLI SECRET KEY VALUES)
    # username = "<ENTER_YOUR_USERNAME>"
    # password = "<ENTER_YOUR_PASSWORD>"
    # bootstrap_servers = "<ENTER_YOUR_BOOTSTRAP_SERVERS>"
    sasl_mechanism = "SCRAM-SHA-256"

    # Ensure the topics exist
    ensure_topics(bootstrap_servers, username, password, sasl_mechanism=sasl_mechanism)

    # Create the callback for trades
    ch_tradekafka = cft.ClickHouseTradeKafka(
        bootstrap=bootstrap_servers,
        username=username,
        password=password,
        sasl_mechanism=sasl_mechanism,
        security_protocol="SASL_SSL"
    )

    # Create the callback for orderbooks
    ch_orderbookkafka = cft.ClickHouseBookKafka(
        bootstrap=bootstrap_servers,
        username=username,
        password=password,
        sasl_mechanism=sasl_mechanism,
        security_protocol="SASL_SSL"
    )

    f = FeedHandler()

    # Add trade feeds
    f.add_feed(Coinbase(channels=[TRADES], symbols=cft.SYMBOLS, callbacks={TRADES: [ch_tradekafka, cft.my_print]}))
    # f.add_feed(Bitstamp(channels=[TRADES], symbols=cft.SYMBOLS, callbacks={TRADES: [ch_tradekafka, cft.my_print]}))
    # f.add_feed(Kraken(channels=[TRADES], symbols=cft.SYMBOLS, callbacks={TRADES: [ch_tradekafka, cft.my_print]}))

    # # Add orderbook feeds
    # f.add_feed(Coinbase(channels=[L2_BOOK], symbols=cft.SYMBOLS, callbacks={L2_BOOK: [ch_orderbookkafka, cft.my_print]}))
    # f.add_feed(Bitstamp(channels=[L2_BOOK], symbols=cft.SYMBOLS, callbacks={L2_BOOK: [ch_orderbookkafka, cft.my_print]}))
    # f.add_feed(Kraken(channels=[L2_BOOK], symbols=cft.SYMBOLS, callbacks={L2_BOOK: [ch_orderbookkafka, cft.my_print]}))

    # Run the FeedHandler
    await f.run()


def main():
    asyncio.run(run_feed_handler())


if __name__ == '__main__':
    main()


# COMMAND ----------


