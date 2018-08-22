import asyncio
import logging
import mysql.connector
from threading import Thread
from time import sleep
from queue import Queue

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

        stmt = "INSERT INTO {0} (timestamp, source_addr, destination_addr, apci, tpci, priority," \
            "repeated, hop_count, apdu, payload_length, cemi, payload_data, is_manipulated) " \
            "VALUES ('{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}');"

        try:
            self.__cursor.execute(stmt.format(self.__db_config.db_table,
                str(telegram.timestamp), str(telegram.source_addr), str(telegram.destination_addr), str(telegram.apci), str(telegram.tpci),
                str(telegram.priority), str(telegram.repeated), telegram.hop_count, str(telegram.apdu), telegram.payload_length,
                str(telegram.cemi), str(telegram.payload_data), telegram.is_manipulated
            ))
            self.__con.commit()
            return True
        except mysql.connector.Error as err:
            LOGGER.error("Failed to insert telegram: {}".format(err))
            return False

