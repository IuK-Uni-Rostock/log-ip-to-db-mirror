#!/usr/bin/env python3

import mysql.connector
import baos_knx_parser as knx_parser
from ctypes import CDLL, CFUNCTYPE, POINTER, c_int, c_void_p, c_uint, c_ubyte, pointer, create_string_buffer
from threading import Thread
from datetime import datetime
from time import sleep
from sys import exit
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
    KDRIVE_EVENT_ERROR = 0x00
    KDRIVE_EVENT_OPENING = 0x01
    KDRIVE_EVENT_OPENED = 0x02
    KDRIVE_EVENT_CLOSING = 0x03
    KDRIVE_EVENT_CLOSED = 0x04
    KDRIVE_EVENT_TERMINATED = 0x05
    KDRIVE_EVENT_KNX_BUS_CONNECTED = 0x06
    KDRIVE_EVENT_KNX_BUS_DISCONNECTED = 0x07
    KDRIVE_EVENT_LOCAL_DEVICE_RESET = 0x08
    KDRIVE_EVENT_TELEGRAM_INDICATION = 0x09
    KDRIVE_EVENT_TELEGRAM_CONFIRM = 0x0A
    KDRIVE_EVENT_TELEGRAM_CONFIRM_TIMEOUT = 0x0B
    KDRIVE_EVENT_INTERNAL_01 = 0x0C
    # KDRIVE DEFINITIONS END

    def __init__(self, queue):
        Thread.__init__(self)
        self.__telegram_queue = queue

    def __print(self, line):
        print("[{0} BUSMON] {1}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), line))

    def run(self):
        self.__print("Starting bus monitor")

        error_callback = self.ERROR_CALLBACK(self.__on_error_callback)
        self.kdrive.kdrive_register_error_callback(error_callback, None)

        self.ap = self.kdrive.kdrive_ap_create()

        if self.ap == -1:
            self.__print("Failed to create access port")
            exit(1)

        event_callback = self.EVENT_CALLBACK(self.__on_event_callback)
        self.kdrive.kdrive_set_event_callback(self.ap, event_callback, None)

        if self.kdrive.kdrive_ap_open_ip(self.ap, knx_gateway_ip) > 0:
            self.__print("Failed to open KNX IP gateway")
            self.kdrive.kdrive_ap_release(self.ap)
            exit(1)

        key = c_int(0)
        telegram_callback = self.TELEGRAM_CALLBACK(self.__on_telegram_callback)
        self.kdrive.kdrive_ap_register_telegram_callback(self.ap, telegram_callback, None, pointer(key))

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
        if e == self.KDRIVE_EVENT_ERROR:
            self.__print("Access port error")
        elif e == self.KDRIVE_EVENT_OPENING:
            self.__print("Access port opening")
        elif e == self.KDRIVE_EVENT_OPENED:
            self.__print("Access port opened")
        elif e == self.KDRIVE_EVENT_CLOSED:
            self.__print("Access port closed")
        elif e == self.KDRIVE_EVENT_CLOSING:
            self.__print("Access port closing")
        elif e == self.KDRIVE_EVENT_TERMINATED:
            self.__print("Access port terminated")
        elif e == self.KDRIVE_EVENT_KNX_BUS_CONNECTED:
            self.__print("KNX bus connected")
        elif e == self.KDRIVE_EVENT_KNX_BUS_DISCONNECTED:
            self.__print("KNX bus disconnected")
        elif e == self.KDRIVE_EVENT_LOCAL_DEVICE_RESET:
            self.__print("Local device reset")
        elif e == self.KDRIVE_EVENT_TELEGRAM_INDICATION:
            self.__print("Telegram indication")
        elif e == self.KDRIVE_EVENT_TELEGRAM_CONFIRM:
            self.__print("Telegram confirm")
        elif e == self.KDRIVE_EVENT_TELEGRAM_CONFIRM_TIMEOUT:
            self.__print("Telegram confirm timeout")
        elif e == self.KDRIVE_EVENT_INTERNAL_01:
            pass
        else:
            self.__print("Unknown kdrive event")

    def __on_error_callback(self, e, user_data):
        len = 1024
        str = create_string_buffer(len)
        self.kdrive.kdrive_get_error_message(e, str, len)
        self.__print('kdrive error: {0}'.format(str.value))

    def stop(self):
        self.__print("Stopping bus monitor")
        self.kdrive.kdrive_ap_close(self.ap)
        self.kdrive.kdrive_ap_release(self.ap)

class DatabaseWriter(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.__telegram_queue = queue

    def __print(self, line):
        print("[{0} DBWRIT] {1}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), line))

    def run(self):
        self.__print("Starting database writer")
        self.__connect_db()
        while True:
            telegram = self.__telegram_queue.get()
            if telegram is None:
                self.__cursor.close()
                self.__con.close()
                break
            while self.__insert_telegram(telegram) == False:
                sleep(5) # wait 5 sec, then try again
                self.__connect_db() # reconnect on insert failure
            self.__telegram_queue.task_done()

    def __connect_db(self):
        try:
            self.__con = mysql.connector.connect(**db_cfg)
            self.__cursor = self.__con.cursor()
        except mysql.connector.Error as err:
            self.__print("Failed to connect to database: {}".format(err))

    def __insert_telegram(self, telegram):
        if self.__con.is_connected is False:
            return False

        stmt = "INSERT INTO {0} (timestamp, source_addr, destination_addr, apci, tpci, priority," \
            "repeated, hop_count, apdu, payload_length, cemi, payload_data, is_manipulated) " \
            "VALUES ('{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}');"

        try:
            self.__cursor.execute(stmt.format(db_table,
                str(telegram.timestamp), str(telegram.source_addr), str(telegram.destination_addr), str(telegram.apci), str(telegram.tpci),
                str(telegram.priority), str(telegram.repeated), telegram.hop_count, str(telegram.apdu), telegram.payload_length,
                str(telegram.cemi), str(telegram.payload_data), telegram.is_manipulated
            ))
            self.__con.commit()
            return True
        except mysql.connector.Error as err:
            self.__print("Failed to insert telegram: {}".format(err))
            return False

    def stop(self):
        self.__print("Stopping database writer")
        # add stop-item to queue
        self.__telegram_queue.put(None)

def main():
    print("Press [Enter] to exit the application ...")
    telegram_queue = Queue()
    threads = []

    busMon = BusMonitor(telegram_queue)
    dbWriter = DatabaseWriter(telegram_queue)
    busMon.start()
    dbWriter.start()

    threads.append(busMon)
    threads.append(dbWriter)

    input('')
    busMon.stop()
    dbWriter.stop()

    for t in threads:
        t.join()
    exit(0)

if __name__ == '__main__':
    main()
