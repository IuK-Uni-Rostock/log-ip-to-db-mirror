import asyncio
import logging
import mysql.connector
from threading import Thread
from time import sleep
from queue import Queue

from knxmap.data.telegram import Telegram, AckTelegram, UnknownTelegram

__all__ = ['DatabaseWriter']

LOGGER = logging.getLogger(__name__)

class DatabaseWriter(Thread):
    def __init__(self, queue, db_config):
        Thread.__init__(self)
        self.__telegram_queue = queue
        self.__db_config = db_config

    def run(self):
        LOGGER.info("Starting database writer")
        self.__connect_db()
        while True:
            telegram = self.__telegram_queue.get()
            if telegram is None:
                self.__cursor.close()
                self.__con.close()
                break
            while self.__insert_telegram(telegram) == False:
                LOGGER.info("Reconnecting after 5 seconds")
                sleep(5) # wait 5 sec, then try again
                self.__connect_db() # reconnect on insert failure
            self.__telegram_queue.task_done()

    def __connect_db(self):
        try:
            self.__con = mysql.connector.connect(**self.__db_config.db_cfg)
            self.__cursor = self.__con.cursor()
            LOGGER.info("Successfully connected to database")
        except mysql.connector.Error as err:
            LOGGER.error("Failed to connect to database: {}".format(err))

    def __insert_telegram(self, telegram):
        if self.__con.is_connected is False:
            return False

        if isinstance(telegram, Telegram):
            stmt = "INSERT INTO {0} (timestamp, source_addr, destination_addr, extended_frame, priority, `repeat`, " \
                   "ack_req, confirm, system_broadcast, hop_count, tpci, tpci_sequence, apci, payload_data, " \
                   "payload_length, is_manipulated, sensor_addr) " \
                   "VALUES ('{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}', '{15}', '{16}', '{17}');"

            try:
                stmt = stmt.format(self.__db_config.db_table,
                       telegram.timestamp, telegram.source_addr, telegram.destination_addr, telegram.extended_frame, telegram.priority, telegram.repeat,
                       telegram.ack_req, telegram.confirm, telegram.system_broadcast, telegram.hop_count, telegram.tpci, telegram.tpci_sequence,
                       telegram.apci, telegram.payload_data, telegram.payload_length, telegram.is_manipulated, self.__db_config.gateway_address)
                LOGGER.debug(stmt)
                self.__cursor.execute(stmt)
                self.__con.commit()
                return True
            except mysql.connector.Error as err:
                LOGGER.error("Failed to insert telegram: {}".format(err))
                return False
        elif isinstance(telegram, AckTelegram):
            # insert acknowledgement telegram to database
            stmt = "INSERT INTO {0} (timestamp, apci, is_manipulated, sensor_addr) " \
                   "VALUES ('{1}', '{2}', '{3}', '{4}');"

            try:
                stmt = stmt.format(self.__db_config.db_table, telegram.timestamp, telegram.apci, telegram.is_manipulated, self.__db_config.gateway_address)
                LOGGER.debug(stmt)
                self.__cursor.execute(stmt)
                self.__con.commit()
                return True
            except mysql.connector.Error as err:
                LOGGER.error("Failed to insert ack telegram: {}".format(err))
                return False
        else:
            # insert unknown telegrams to different table
            stmt = "INSERT INTO unknown_telegram (timestamp, cemi, sensor_addr) VALUES ('{0}', '{1}', '{2}')"
            try:
                stmt = stmt.format(telegram.timestamp, telegram.cemi, self.__db_config.gateway_address)
                LOGGER.debug(stmt)
                self.__cursor.execute(stmt)
                self.__con.commit()
                return True
            except mysql.connector.Error as err:
                LOGGER.error("Failed to insert unknown telegram: {}".format(err))
                return False
