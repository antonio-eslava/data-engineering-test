"""
This script takes five files: 'Part0.tsv' to 'Part4.tsv'.
Those files are the result of splitting the original 'data.tsv' file.
The script treats each file separately and returns three files:
 - A main file, where the records with an 0Ah wrapped are into quotation marks.
 - A start file, with the first record (partial).
 - An end file, with the last record (partial).
After that, the script reunites all the files sequentially in a final one.

This is a proof of concept. For use it in a true parallelized envronment,
 it would be enough to take the functions and distribute them.
"""

import codecs
import os
     
def isEndRec(readFile, id, startCheck):
    field = ""
    for j, c in enumerate(readFile[startCheck::2]):
        if c != 9:
            if not chr(c).isdigit():
                return False
            else:
                field = field + chr(c)
        else:
            if field == "":
                return False
            else:
                if id == "id":
                    return True
                elif int(id) == int(field) - 1:
                    return True
                else:
                    return False      

def firstRec(readFile, startFile, startPos):
    fields = {}
    field = ""
    f = 0
    for i, b in enumerate(readFile[startPos::2]):
        if f > 15:
            break
        if b == 9 or b == 10:
            fields[f] = (field,b)
            field = ""
            f += 1
            continue
        field = field + chr(b)

    cont = -1
    for k, v in fields.items():
        if k < 5:
            if v[1] != 10:
                continue
            else:
                nextId = int(fields[k + 1][0])
                if nextId == int(fields[k + 6][0]) - 1:
                    if fields[0][0] == "id":
                        startFile.write("id\t")
                        cont += 3
                    else:
                        startFile.write(str(nextId - 1))
                        startFile.write("\t")
                    for c in range(1, 5):
                        if k + c < 4:
                            startFile.write("")
                            startFile.write("\t")
                        else:
                            startFile.write(fields[c + k - 4][0])
                            cont = cont + len(fields[c + k - 4][0])
                            if c != 4:
                                startFile.write("\t")
                            else:
                                startFile.write("\n")
                            cont += 1
    startPos = (cont * 2) + 1
    return(startPos)

def mainRecs(readFile, mainFile, startPos):
    startPos = startPos + 1
    currPos = startPos
    fields = {}
    field = ""
    f = 0
    quotes = False
    isEnd = True
    for i, b in enumerate(readFile[startPos::2]):
        if b == 9:
            currPos = currPos + 2 * (len(field) + 1)
            if quotes:
                field = '"' + field + '"'
                quotes = False
            fields[f] = (field,b)
            field = ""
            f += 1
            continue
        if b != 10:
            field = field + chr(b)
            continue
        else:
            startNextRec = startPos + (i * 2) + 2
            id = fields[0][0]
            isEnd = isEndRec(readFile, id, startNextRec)
            if not isEnd:
                quotes = True
                field = field + chr(b)
            else:
                currPos = currPos + 2 * (len(field) + 1)
                fields[f] = (field,b)
                for k in range(0, 5):
                    mainFile.write(fields[k][0])
                    mainFile.write(chr(fields[k][1]))
                fields = {}
                field = ""
                f = 0
    b = 9
    fields[f] = (field, b)
    return(startNextRec,fields)

def lastRec(readFile, endFile, startPos, fields):
    for k in range(0, 5):
        tab = "\t"
        nl = "\n"
        if k in fields:
            endFile.write(fields[k][0])
            endFile.write(tab)
        else:
            blank = ""
            endFile.write(blank)
            if k == 4:
                endFile.write(nl)             
            else:
                endFile.write(tab)
    return
    
def fixPartialFile(inFilename, startFilename, mainFilename, endFilename):
    with open(inFilename, "rb") as inFile:
            readFile = inFile.read()
            startPos = 0
            it = iter(readFile)
            for b in it:
                if next(it) != 0 and b == 0:
                    startPos = 1
                    break
                break

    inFile  = codecs.open(inFilename,'rb')
    readFile = inFile.read()
    startFile = codecs.open(startFilename, 'w', encoding='utf-8')
    mainFile = codecs.open(mainFilename, 'w', encoding='utf-8')
    endFile = codecs.open(endFilename, 'w', encoding='utf-8')


    startPos = firstRec(readFile, startFile, startPos)
    startPos, fields = mainRecs(readFile, mainFile, startPos)    
    lastRec(readFile, endFile, startPos, fields)                

    inFile.close
    startFile.close
    mainFile.close
    endFile.close
    return

def mixEndStart(endFilename, startFilename):
    endFile  = open(endFilename,'r')
    endFields = endFile.read().strip("\n").split("\t")
    startFile  = open(startFilename,'r')    
    startFields = startFile.read().split("\t")

    field = ""
    for i in range(0,5):
        if i == 0:
            outFile.write(endFields[i])               
        else:
            outFile.write(endFields[i])
            outFile.write(startFields[i])
        if i < 4:
            outFile.write("\t") 
    endFile.close()
    startFile.close()
   
# Main function
n = 0
for inFilename in os.listdir():
    if inFilename.startswith("Part") and inFilename.endswith(".tsv"):
        startFilename = "fixedStart" + str(n) + ".tsv"
        mainFilename = "fixedMain" + str(n) + ".tsv"
        endFilename = "fixedEnd" + str(n) + ".tsv"
        n += 1
        fixPartialFile(inFilename, startFilename, mainFilename, endFilename)

outFile = open("data_etl_1.tsv", "w")
for n in range(0, 5):
    if n == 0:
        inFile = open("fixedStart0.tsv", "r")
        for l in inFile:
            outFile.write(l)
    inFile = open("fixedMain" + str(n) + ".tsv", "r")
    for l in inFile:
        outFile.write(l)
    if n < 4:
        endFilename = "fixedEnd" + str(n) + ".tsv"
        startFilename = "fixedStart" + str(n+1) + ".tsv"
        mixEndStart(endFilename, startFilename)
    else:
        inFile = open("fixedEnd" + str(n) + ".tsv", "r")
        for l in inFile:
            outFile.write(l)
outFile.close()
    
    

    
    
