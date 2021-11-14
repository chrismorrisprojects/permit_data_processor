from definitions.texas.ebdicFormats import pic_yyyymmdd, pic_yyyymm, pic_numeric, pic_any, pic_signed, comp3

def yield_blocks(file, n):
    block_bytes = file.read(n)
    while block_bytes:
        yield block_bytes
        block_bytes = file.read(n)


def parse_record(record, layout):
    values = dict()

    for name, start, size, convert in layout:
        decimal = 0
        ##check for additional data for pic_signed and comp 3 methods
        if '_' in str(size):  ##check if size also includes the number of decimals "Size_Decimal"
            size_split = size.split('_')
            size = int(size_split[0])
            decimal = int(size_split[1])

        if convert == 'pic_yyyymmdd':
            values[name] = pic_yyyymmdd(record[start:start + size])
            if values[name] == None:
                values[name] = '01/01/1900'
        elif convert == 'pic_yyyymm':
            values[name] = pic_yyyymm(record[start:start + size])
            if values[name] == None:
                values[name] = '01/01/1900'
        elif convert == 'pic_numeric':
            values[name] = pic_numeric(record[start:start + size])
        elif convert == 'pic_signed':
            values[name] = pic_signed(record[start:start + size], name, decimal)
        elif convert == 'pic_comp':
            values[name] = comp3(record[start:start + size], decimal)
        else:
            values[name] = pic_any(record[start:start + size])
            if values[name] == None:
                values[name] = ''

    return values