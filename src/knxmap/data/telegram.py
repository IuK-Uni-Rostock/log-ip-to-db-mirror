__all__ = ['Telegram', 'AckTelegram', 'UnknownTelegram']

class Telegram(object):
    sequence_number = None
    timestamp = None
    source_addr = None
    destination_addr = None
    extended_frame = None
    priority = None
    repeat = None
    ack_req = None
    confirm = None
    system_broadcast = None
    hop_count = None
    tpci = None
    tpci_sequence = None
    apci = None
    payload_data = None
    payload_length = None
    is_manipulated = None
    attack_type_id = None
    sensor_addr = None

class AckTelegram(object):
    sequence_number = None
    timestamp = None
    apci = None
    is_manipulated = None
    attack_type_id = None
    sensor_addr = None

class UnknownTelegram(object):
    sequence_number = None
    timestamp = None
    cemi = None
    sensor_addr = None
