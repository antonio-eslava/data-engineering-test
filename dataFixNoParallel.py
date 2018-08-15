"""
This script takes the file 'data.tsv' and creates a new one, 'data_etl_1.tsv'.
In this, the fields with a 0Ah character inside are wrapped in quotation marks.
"""

import codecs

infilename = 'data.tsv'
outfilename = 'data_etl_1.tsv'

infile  = codecs.open(infilename,'rb', encoding='utf-16-le')
outfile = codecs.open(outfilename, 'wb', encoding='utf-8')

with open(infilename, "rb") as f:
    
    raw_file = f.read()
    field = ""
    quotes = False
    char = True
    t = 0
    for b in raw_file:
        byte = chr(b)
        if char != True:
            char = True
            continue
        elif b == 10:
            if t < 4:
                quotes = True
                field = field + chr(b)
                char = False
                continue
            else:
                outfile.write(field)
                outfile.write("\n")
                t = 0
                char = False
                field = ""
                continue
        elif b == 9:
            if quotes:
                field = '"' + field + '"'
                quotes = False
            outfile.write(field)
            outfile.write("\t")
            t += 1
            char = False
            field = ""
            continue
        field = field + chr(b)
        char = False

infile.close()
outfile.close()

print("Finished")
