#!/usr/bin/env python3

import csv, os

def readFile(filename):
    with open(filename,'r',encoding='latin1') as csvDataFile:
        try:
            csvReader = csv.reader(csvDataFile)
            for row in csvReader:
                yield row
        except:
            print('failed in file ' + filename)
        finally:
            csvDataFile.close()

def writeFile(filename, csvData):
    with open(filename, 'w', newline='',encoding='latin1') as csvNewFile:
        try:
            csvWriter = csv.writer(csvNewFile)
            for row in csvData:
                csvWriter.writerow(row)
        except:
            print('failed to write file ' + filename)
        finally:
            csvNewFile.close()


def importCsvData(filename):
    csvData = []
    for row in readFile(filename):
        csvData.append(row)

    return csvData

def importAuditData(auditDirPath):
    auditFiles = os.listdir(auditDirPath)
    auditFiles.sort()
    auditFiles.reverse()
    for filename in auditFiles:
        if int(filename[0:4]) > 2010:
            importedRows = importCsvData(auditDirPath + '/' + filename)
            auditRows = list(filter(lambda r: r[0] == 'D' and r[1] == '7', importedRows))
            yield (filename, auditRows)

def buildStockData(dbPath):
    pluData = {}
    stockData = {}
    rejectedRows = []
    pluCols = {
        'barcode': 0,
        'dept': 1,
        'name': 2,
        'price': 3
    }

    stockCols = {
        'code': 0,
        'name': 1,
        'dept': 2,
        'supplier': 15,
        'price': 19
    }

    auditCols = {
        'barcode': 4,
        'name': 6,
        'price': 8,
        'code': 9
    }
    stockRows = importCsvData(dbPath + '/Stock.cleaned.dat')
    print("starting Stock data length: {0}".format(len(stockRows)))
    for stockRow in stockRows:
        stockData[stockRow[stockCols['code']]] = stockRow
    pluRows = importCsvData(dbPath + '/plu.csv')
    for pluRow in pluRows:
        pluData[str(pluRow[pluCols['barcode']]).strip()] = pluRow
    print("Plu data length: {0}".format(len(pluRows)))

    auditRowCount = 0
    for (filename, auditRows) in importAuditData(dbPath + '/Sp001/Audit'):
        auditRowCount+= len(auditRows)
        for auditRow in auditRows:
            try:
                barcode = str(auditRow[auditCols['barcode']]).lstrip('0')
                if stockData.get(auditRow[auditCols['code']], None) == None:
                    if pluData.get(barcode, None) != None:
                        stockRow = [''] * 21
                        stockRow[stockCols['code']] = auditRow[auditCols['code']]
                        stockRow[stockCols['name']] = auditRow[auditCols['name']]
                        stockRow[stockCols['dept']] = pluData.get(barcode, [''] * 4)[pluCols['dept']]
                        stockRow[stockCols['price']] = auditRow[auditCols['price']]
                        stockRow[3] = 8
                        stockRow[5] = 1
                        stockRow[12] = 1
                        stockRow[14] = 1
                        stockRow[17] = 1
                        stockRow[18] = 1
                        stockRow[20] = auditRow[auditCols['code']]
                        stockData[auditRow[auditCols['code']]] = stockRow
                    else:
                        rejectedRows.append(auditRow)
            except:
                print('Failed in file ' + filename + 'with data:')
                print(auditRow)

    print("Audited Rows Count: {0}".format(auditRowCount))
    print("Ending Stock data length: {0}".format(len(stockData)))

    stockRows = []
    for code in stockData:
        stockRows.append(stockData[code])

    writeFile('Stock.new.csv', stockRows)

    writeFile('Stock.rejected.csv', rejectedRows)



if __name__ == '__main__':
    dbPath = str(input('Path to Db001: '))
    buildStockData(dbPath)
