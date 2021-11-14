import codecs
from datetime import datetime
from datetime import date
from array import array

def ebc_decode(data):
    ebcdic_decoder = codecs.getdecoder('cp1140')
    decoded = ebcdic_decoder(data)
    val = decoded[0]
    return val


def pic_yyyymmdd(date):
    date = ebc_decode(date)
    # Changes format YYYYMMDD from a series of numbers to datetime object
    try:
        val = datetime.strptime(date, '%Y%m%d').strftime('%m/%d/%Y')
    except ValueError:
        val = None
    return val


def pic_yyyymm(yyyymm):
    yyyymm = ebc_decode(yyyymm)
    try:
        val = date(year=int(yyyymm[0:4]), month=int(yyyymm[4:]), day=1).strftime('%m/01/%Y')
    except ValueError:
        val = None

    return val


def pic_numeric(num):
    num = ebc_decode(num)
    try:
        val = int(num)
    except:
        val = None

    return val


def pic_any(string):
    string = ebc_decode(string)
    STRIP_PIC_X = True
    val = str(string)
    if STRIP_PIC_X == True:
        val = val.strip()

    return val


def pic_signed(signed, name, decimal=0):
    signed_raw = array('B', signed);
    val = float(0);
    for i in signed_raw:
        val = val * 10 + (i & 0x0F)

    val = (val * (-1 if signed_raw[-1] >> 4 == 0xD else 1)) / 10 ** decimal
    if 'LONGITUDE' in name and val > 0:
        val = -val

    return val


def comp3(packed, decimal_location=0):
    bin_arr = array('B', packed)
    val = float(0)
    for i in bin_arr[:-1]:
        val = (val * 100) + (((i & 0xf0) >> 4) * 10) + (i & 0xf)

    i = bin_arr[-1]
    val = (val * 10) + ((i & 0xf0) >> 4)
    if (i & 0xf) == 0xd:
        val = -val

    val = val / (10 ** decimal_location)
    if decimal_location == 0:
        val = int(val)

    return val