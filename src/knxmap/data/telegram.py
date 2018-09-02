__all__ = ['Telegram', 'AckTelegram']

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

class AckTelegram(object):
    sequence_number = None
    timestamp = None
    apci = None
    cemi = None
    is_manipulated = None
