import logging
import codecs
import importlib
import sys
from queue import Queue
from datetime import datetime

import bitstruct
import baos_knx_parser as knx_parser

from knxmap.database import DatabaseWriter
from knxmap.bus.tunnel import KnxTunnelConnection
from knxmap.data.telegram import Telegram, AckTelegram, UnknownTelegram
from knxmap.data.constants import *
from knxmap.messages import parse_message, KnxConnectRequest, KnxConnectResponse, \
                            KnxTunnellingRequest, KnxTunnellingAck, KnxConnectionStateResponse, \
                            KnxDisconnectRequest, KnxDisconnectResponse

LOGGER = logging.getLogger(__name__)


class KnxBusMonitor(KnxTunnelConnection):
    """Implementation of bus_monitor_mode and group_monitor_mode."""
    def __init__(self, future, loop=None, group_monitor=True, db_config=None):
        super(KnxBusMonitor, self).__init__(future, loop=loop)
        self.group_monitor = group_monitor
        if db_config is not None:
            sys.path.insert(0, db_config)
            self.db_config = importlib.import_module('config')
            self.telegram_queue = Queue()
            self.dbWriter = DatabaseWriter(self.telegram_queue, self.db_config)
            self.dbWriter.start()
        else:
            self.db_config = None

    def connection_made(self, transport):
        self.transport = transport
        self.peername = self.transport.get_extra_info('peername')
        self.sockname = self.transport.get_extra_info('sockname')
        if self.group_monitor:
            # Create a TUNNEL_LINKLAYER layer request (default)
            connect_request = KnxConnectRequest(sockname=self.sockname)
        else:
            # Create a TUNNEL_BUSMONITOR layer request
            connect_request = KnxConnectRequest(sockname=self.sockname,
                                                layer_type='TUNNEL_BUSMONITOR')
        LOGGER.trace_outgoing(connect_request)
        self.transport.sendto(connect_request.get_message())
        # Send CONNECTIONSTATE_REQUEST to keep the connection alive
        self.loop.call_later(50, self.knx_keep_alive)

    def datagram_received(self, data, addr):
        knx_message = parse_message(data)
        if not knx_message:
            LOGGER.error('Invalid KNX message: {}'.format(data))
            self.knx_tunnel_disconnect()
            self.transport.close()
            self.future.set_result(None)
            return
        knx_message.set_peer(addr)
        LOGGER.trace_incoming(knx_message)
        if isinstance(knx_message, KnxConnectResponse):
            if not knx_message.ERROR:
                if not self.tunnel_established:
                    self.tunnel_established = True
                self.communication_channel = knx_message.communication_channel
            else:
                if not self.group_monitor and knx_message.ERROR_CODE == 0x23:
                    LOGGER.error('Device does not support BUSMONITOR, try --group-monitor instead')
                else:
                    LOGGER.error('Connection setup error: {}'.format(knx_message.ERROR))
                self.transport.close()
                self.future.set_result(None)
        elif isinstance(knx_message, KnxTunnellingRequest):
            self.print_message(knx_message)
            self.enqueue_message(data, knx_message)
            if CEMI_PRIMITIVES[knx_message.cemi.message_code] == 'L_Data.con' or \
                    CEMI_PRIMITIVES[knx_message.cemi.message_code] == 'L_Data.ind' or \
                    CEMI_PRIMITIVES[knx_message.cemi.message_code] == 'L_Busmon.ind':
                tunnelling_ack = KnxTunnellingAck(
                    communication_channel=knx_message.communication_channel,
                    sequence_count=knx_message.sequence_counter)
                LOGGER.trace_outgoing(tunnelling_ack)
                self.transport.sendto(tunnelling_ack.get_message())
        elif isinstance(knx_message, KnxTunnellingAck):
            self.print_message(knx_message)
            #self.enqueue_message(knx_message)
        elif isinstance(knx_message, KnxConnectionStateResponse):
            # After receiving a CONNECTIONSTATE_RESPONSE schedule the next one
            self.loop.call_later(50, self.knx_keep_alive)
        elif isinstance(knx_message, KnxDisconnectRequest):
            connect_response = KnxDisconnectResponse(communication_channel=self.communication_channel)
            self.transport.sendto(connect_response.get_message())
            self.transport.close()
            self.future.set_result(None)
        elif isinstance(knx_message, KnxDisconnectResponse):
            self.transport.close()
            self.future.set_result(None)

    def print_message(self, message):
        """A generic message printing function. It defines
        a format for the monitoring modes."""
        assert isinstance(message, KnxTunnellingRequest)
        cemi = tpci = apci= {}
        if message.cemi:
            cemi = message.cemi
            if cemi.tpci:
                tpci = cemi.tpci
                if cemi.apci:
                    apci = cemi.apci
        if cemi.knx_destination and cemi.extended_control_field and \
                cemi.extended_control_field.get('address_type'):
            dst_addr = message.parse_knx_group_address(cemi.knx_destination)
        elif cemi.knx_destination:
            dst_addr = message.parse_knx_address(cemi.knx_destination)
        if self.group_monitor:
            format = ('[ chan_id: {chan_id}, seq_no: {seq_no}, message_code: {msg_code}, '
                      'source_addr: {src_addr}, dest_addr: {dst_addr}, tpci_type: {tpci_type}, '
                      'tpci_seq: {tpci_seq}, apci_type: {apci_type}, apci_data: {apci_data} ]').format(
                chan_id=message.communication_channel,
                seq_no=message.sequence_counter,
                msg_code=CEMI_PRIMITIVES.get(cemi.message_code),
                src_addr=message.parse_knx_address(cemi.knx_source),
                dst_addr=dst_addr,
                tpci_type=_CEMI_TPCI_TYPES.get(tpci.tpci_type),
                tpci_seq=tpci.sequence,
                apci_type=_CEMI_APCI_TYPES.get(apci.apci_type),
                apci_data=apci.apci_data)
        else:
            format = ('[ chan_id: {chan_id}, seq_no: {seq_no}, message_code: {msg_code}, '
                      'timestamp: {timestamp}, raw_frame: {raw_frame} ]').format(
                chan_id=message.communication_channel,
                seq_no=message.sequence_counter,
                msg_code=CEMI_PRIMITIVES.get(cemi.message_code),
                timestamp=codecs.encode(cemi.additional_information.get('timestamp'), 'hex'),
                raw_frame=codecs.encode(cemi.raw_frame, 'hex'))

        LOGGER.info(format)


    def enqueue_message(self, data, message):
        if self.db_config is not None and not self.group_monitor:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            try:
                # remove additional cEMI data
                header_len = bitstruct.unpack('>u8', data[0:1])[0]
                add_len = bitstruct.unpack('>u8', data[header_len:header_len+1])[0]
                cemi = data[header_len+add_len:]

                # parse cEMI and create Telegram/AckTelegram object
                parsed_telegram = knx_parser.parse_knx_telegram(cemi)
                if isinstance(parsed_telegram, knx_parser.KnxBaseTelegram):
                    t = Telegram()
                    t.timestamp = str(timestamp)
                    t.source_addr = str(parsed_telegram.src)
                    t.destination_addr = str(parsed_telegram.dest)
                    t.extended_frame = 1 if parsed_telegram.frame_type == knx_parser.const.FrameType.EXTENDED_FRAME else 0
                    t.priority = str(parsed_telegram.priority)
                    t.repeat = int(parsed_telegram.repeat)
                    t.ack_req = int(parsed_telegram.ack_req)
                    t.confirm = int(parsed_telegram.confirm)
                    t.system_broadcast = int(parsed_telegram.system_broadcast)
                    t.hop_count = int(parsed_telegram.hop_count)
                    t.tpci = str(parsed_telegram.tpci[0])
                    t.tpci_sequence = int(parsed_telegram.tpci[1])
                    t.apci = str(parsed_telegram.apci)
                    t.payload_data = str(parsed_telegram.payload_data)
                    t.payload_length = int(parsed_telegram.payload_length)
                    t.is_manipulated = 0
                    self.telegram_queue.put(t)
                else:
                    t = AckTelegram()
                    t.timestamp = str(timestamp)
                    t.apci = str(parsed_telegram.acknowledgement)
                    t.is_manipulated = 0
                    self.telegram_queue.put(t)
            except Exception as ex:
                LOGGER.error("Failed to parse telegram: {0} with following exception: {1}".format(str(message.cemi.raw_frame.hex()), ex))
                t = UnknownTelegram()
                t.timestamp = str(timestamp)
                t.cemi = str(message.cemi.raw_frame.hex())
                self.telegram_queue.put(t)
