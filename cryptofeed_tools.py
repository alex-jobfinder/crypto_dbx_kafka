import asyncio
import ssl
from collections import OrderedDict

import orjson
from aiokafka import AIOKafkaProducer

# SYMBOLS = [
#     "ALEO-USD",
#     "APT-USD",
#     "ATOM-USD",
#     "AVAX-USD",
#     "AXL-USD",
#     "BTC-USD",
#     "DOT-USD",
#     "EIGEN-USD",
#     "ETH-USD",
#     "FLOW-USD",
#     "HBAR-USD",
#     "MATIC-USD",
#     "NEAR-USD",
#     "OSMO-USD",
#     "SOL-USD",
#     "SUI-USD",
#     "TIA-USD",
#     "UNI-USD",
#     "XRP-USD",
#     "XTZ-USD",
#     "ZETA-USD"
# ]
SYMBOLS = ['ALEO-USD', 'APT-USD', 'ATOM-USD', 'AVAX-USD', 'AXL-USD', 'BTC-USD', 'DOT-USD', 'EIGEN-USD', 'ETH-USD', 'FLOW-USD', 'HBAR-USD', 'MATIC-USD', 'NEAR-USD', 'OSMO-USD', 'SOL-USD', 'SUI-USD', 'TIA-USD', 'UNI-USD', 'XRP-USD', 'XTZ-USD', 'ZETA-USD']

# SYMBOLS = ['BTC-USD', 'ETH-USD', 'AVAX-USD', 'SOL-USD']


async def my_print(data, _receipt_time):
    print(data)


class KafkaCallback:
    def __init__(self, bootstrap, topic=None, numeric_type=float, none_to=None, sasl_mechanism="SCRAM-SHA-256",
                 username=None, password=None, security_protocol="SASL_SSL", ssl_cafile=None, ssl_certfile=None, ssl_keyfile=None, **kwargs):
        """
        bootstrap: str
            The bootstrap server(s). e.g. 'yourcluster:9092'
        """
        self.bootstrap = bootstrap
        self.topic = topic if topic else self.default_topic
        self.numeric_type = numeric_type
        self.none_to = none_to
        self.username = username
        self.password = password
        self.sasl_mechanism = sasl_mechanism
        self.security_protocol = security_protocol
        self.ssl_cafile = ssl_cafile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.producer = None

    async def __call__(self, dtype, receipt_timestamp: float):
        if isinstance(dtype, dict):
            data = dtype
        else:
            data = dtype.to_dict(numeric_type=self.numeric_type, none_to=self.none_to)
            if not dtype.timestamp:
                data['timestamp'] = receipt_timestamp
            data['receipt_timestamp'] = receipt_timestamp
        await self.write(data)

    async def __connect(self):
        if not self.producer:
            loop = asyncio.get_event_loop()
            ssl_context = None
            if self.security_protocol in ("SSL", "SASL_SSL"):
                ssl_context = ssl.create_default_context(cafile=self.ssl_cafile)
                if self.ssl_certfile and self.ssl_keyfile:
                    ssl_context.load_cert_chain(certfile=self.ssl_certfile, keyfile=self.ssl_keyfile)

            self.producer = AIOKafkaProducer(
                acks=0,
                loop=loop,
                bootstrap_servers=self.bootstrap,
                client_id='cryptofeed',
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.username,
                sasl_plain_password=self.password,
                ssl_context=ssl_context
            )
            await self.producer.start()

    async def write(self, data: dict):
        await self.__connect()
        await self.producer.send_and_wait(self.topic, orjson.dumps(data))


class ClickHouseTradeKafka(KafkaCallback):
    default_topic = 'trade'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['size'] = data.pop('amount')
            data['trade_id'] = data.pop('id')
            del data['type']
            await self.producer.send_and_wait(self.topic, orjson.dumps(data))
        except:
            print("WARNING: ClickHouseTradeKafka.write() didn't fire - go check!")
            pass


class ClickHouseBookKafka(KafkaCallback):
    default_topic = 'orderbooks'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['bid'] = OrderedDict(sorted(data['book'].pop('bid').items(), reverse=True))
            data['ask'] = OrderedDict(sorted(data['book'].pop('ask').items()))
            del data['book']
            del data['delta']
            await self.producer.send_and_wait(self.topic, orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS))
        except:
            print("WARNING: ClickHouseBookKafka.write() didn't fire - go check!")
            pass


# import asyncio
# from collections import OrderedDict

# import orjson
# from aiokafka import AIOKafkaProducer

# SYMBOLS = ['BTC-USD', 'ETH-USD', 'AVAX-USD', 'SOL-USD']


# async def my_print(data, _receipt_time):
#     print(data)


# class KafkaCallback:
#     def __init__(self, bootstrap='127.0.0.1', port=9092, topic=None, numeric_type=float, none_to=None,
#                  **kwargs):  # working locally
#         """
#         bootstrap: str, list
#             if a list, should be a list of strings in the format: ip/host:port, i.e.
#                 192.1.1.1:9092
#                 192.1.1.2:9092
#                 etc
#             if a string, should be ip/port only
#         """
#         self.bootstrap = bootstrap
#         self.port = port
#         self.producer = None
#         self.topic = topic if topic else self.default_topic
#         self.numeric_type = numeric_type
#         self.none_to = none_to

#     async def __call__(self, dtype, receipt_timestamp: float):
#         if isinstance(dtype, dict):
#             data = dtype
#         else:
#             data = dtype.to_dict(numeric_type=self.numeric_type, none_to=self.none_to)
#             if not dtype.timestamp:
#                 data['timestamp'] = receipt_timestamp
#             data['receipt_timestamp'] = receipt_timestamp
#         await self.write(data)

#     async def __connect(self):
#         if not self.producer:
#             loop = asyncio.get_event_loop()
#             self.producer = AIOKafkaProducer(acks=0,
#                                              loop=loop,
#                                              bootstrap_servers=f'{self.bootstrap}:{self.port}' if isinstance(self.bootstrap, str) else self.bootstrap,
#                                              client_id='cryptofeed')
#             await self.producer.start()

#     async def write(self, data: dict):
#         await self.__connect()
#         await self.producer.send_and_wait(self.topic, orjson.dumps(data).encode('utf-8'))


# class ClickHouseTradeKafka(KafkaCallback):
#     default_topic = 'trades'

#     async def write(self, data: dict):
#         await self._KafkaCallback__connect()
#         try:
#             data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
#             data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
#             data['size'] = data.pop('amount')
#             data['trade_id'] = data.pop('id')
#             del data['type']
#             await self.producer.send_and_wait(self.topic, orjson.dumps(data))  # orjson uses UTF-8 encoding by default
#         except:
#             print("WARNING: ClickHouseTradeKafka.write() didn't fire - go check!")
#             pass


# class ClickHouseBookKafka(KafkaCallback):
#     default_topic = 'orderbooks'

#     async def write(self, data: dict):
#         await self._KafkaCallback__connect()
#         try:
#             data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
#             data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
#             data['bid'] = OrderedDict(sorted(data['book'].pop('bid').items(), reverse=True))
#             data['ask'] = OrderedDict(sorted(data['book'].pop('ask').items()))
#             del data['book']
#             del data['delta']
#             await self.producer.send_and_wait(self.topic, orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS))  # orjson uses UTF-8 encoding by default
#         except:
#             print("WARNING: ClickHouseBookKafka.write() didn't fire - go check!")
#             pass
