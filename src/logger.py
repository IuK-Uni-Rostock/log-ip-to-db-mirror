#!/usr/bin/env python3

import mysql.connector
import baos_knx_parser as knx_parser
from ctypes import CDLL, CFUNCTYPE, POINTER, c_int, c_void_p, c_uint, c_ubyte, pointer, create_string_buffer
from threading import Thread
from datetime import datetime
from config import db_cfg, db_table, knx_gateway_ip
from queue import Queue

class Telegram(object):
    sequence_number = None
    timestamp = None
    source_addr = None
    destination_addr = None
    apci = None
    tpci = None
    priority = None
    repeated = None
    hop_count = None
    apdu = None
    payload_length = None
    cemi = None
    payload_data = None
    is_manipulated = None
    attack_type_id = None

class BusMonitor(Thread):
    # KDRIVE DEFINITIONS START
    kdrive = CDLL('/usr/local/lib/libkdriveExpress.so')
    TELEGRAM_CALLBACK = CFUNCTYPE(None, POINTER(c_ubyte), c_uint, c_void_p)
    EVENT_CALLBACK = CFUNCTYPE(None, c_int, c_uint, c_void_p)
    ERROR_CALLBACK = CFUNCTYPE(None, c_int, c_void_p)
    KDRIVE_LOGGER_FATAL = 1
    KDRIVE_LOGGER_INFORMATION = 6
    # KDRIVE DEFINITIONS END

    def __init__(self, queue):
        Thread.__init__(self)
        self.__telegram_queue = queue

    def run(self):
        print("Starting bus monitor")
        #self.kdrive.kdrive_logger_set_level(self.KDRIVE_LOGGER_INFORMATION)
        #self.kdrive.kdrive_logger_console()

        error_callback = self.ERROR_CALLBACK(self.__on_error_callback)
        self.kdrive.kdrive_register_error_callback(error_callback, None)

        ap = self.kdrive.kdrive_ap_create()

        if ap == -1:
            print("Failed to create access port")
            exit(1)

        event_callback = self.EVENT_CALLBACK(self.__on_event_callback)
        self.kdrive.kdrive_set_event_callback(ap, event_callback, None)

        if self.kdrive.kdrive_ap_open_ip(ap, knx_gateway_ip) > 0:
            print("Failed to open KNX IP gateway")
            self.kdrive.kdrive_ap_release(ap)
            exit(1)

        key = c_int(0)
        telegram_callback = self.TELEGRAM_CALLBACK(self.__on_telegram_callback)
        self.kdrive.kdrive_ap_register_telegram_callback(ap, telegram_callback, None, pointer(key))
        i = raw_input('')

        self.kdrive.kdrive_ap_close(ap)
        self.kdrive.kdrive_ap_release(ap)

    def __on_telegram_callback(self, telegram, telegram_len, user_data):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        cemi = bytearray(telegram)
        parsed_telegram = knx_parser.parse_knx_telegram(cemi)

        t = Telegram()
        t.timestamp = timestamp
        t.source_addr = parsed_telegram.src
        t.destination_addr = parsed_telegram.dest
        t.apci = parsed_telegram.apci
        t.tpci = parsed_telegram.tpci
        t.priority = parsed_telegram.priority
        t.repeated = parsed_telegram.repeat
        t.hop_count = parsed_telegram.hop_count
        t.apdu = parsed_telegram.payload.hex()
        t.payload_length = parsed_telegram.payload_length
        t.cemi = cemi
        t.payload_data = parsed_telegram.payload_data
        t.attack_type_id = 'NULL'

        self.__telegram_queue.put(t)

    def __on_event_callback(self, ap, e, user_data):
        print('kdrive event {0}'.format(hex(e)))

    def __on_error_callback(self, e, user_data):
        len = 1024
        str = create_string_buffer(len)
        self.kdrive.kdrive_get_error_message(e, str, len)
        print('kdrive error {0} {1}'.format(hex(e), str.value))

class DatabaseWriter(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.__telegram_queue = queue

    def run(self):
        print("Starting database writer")
        self.__connect_db()
        while True:
            telegram = self.__telegram_queue.get()
            while self.__insert_telegram(telegram) == False:
                self.__connect_db() # reconnect on insert failure
            self.__insert_telegram(telegram)
            self.__telegram_queue.task_done()

    def __connect_db(self):
        try:
            self.__con = mysql.connector.connect(**db_cfg)
            self.__cursor = self.__con.cursor()
        except mysql.connector.Error as err:
            print("Failed to connect to database: {}".format(err))

    def __insert_telegram(self, telegram):
        if self.__cursor is None:
            return False

        stmt = 'INSERT INTO %s (timestamp, source_addr, destination_addr, apci, tpci, priority,' \
            'repeated, hop_count, apdu, payload_length, cemi, payload_data, is_manipulated) ' \
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'

        try:
            self.__cursor.execute(stmt.format(db_table,
                telegram.timestamp, telegram.source_addr, telegram.destination_addr, telegram.apci, telegram.tpci,
                telegram.priority, telegram.repeated, telegram.hop_count, telegram.apdu, telegram.payload_length,
                telegram.cemi, telegram.payload_data, telegram.is_manipulated
            ))
            return True
        except mysql.connector.Error as err:
            print("Failed to insert telegram: {}".format(err))
            return False

def main():
    telegram_queue = Queue()
    threads = []

    busMon = BusMonitor(telegram_queue)
    dbWriter = DatabaseWriter(telegram_queue)
    busMon.start()
    dbWriter.start()

    threads.append(busMon)
    threads.append(dbWriter)

    #TODO: Add a way to gracefully stop this
    for t in threads:
        t.join()
    exit(0)

if __name__ == '__main__':
    main()
