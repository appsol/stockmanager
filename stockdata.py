#!/usr/bin/env python3

import csv, os

stockData = []
pluData = []
categoryData = []
supplierData = {}

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
    'cost': 19
}

auditCols = {
    'barcode': 4,
    'name': 6,
    'price': 8,
    'code': 9
}

supplierCols = {
    'key': 0,
    'code': 1,
    'name': 3
}

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

def importAuditFileData(auditDirPath, oldestYear):
    auditFiles = os.listdir(auditDirPath)
    auditFiles.sort()
    auditFiles.reverse()
    for filename in auditFiles:
        if int(filename[0:4]) > oldestYear:
            importedRows = importCsvData(auditDirPath + '/' + filename)
            auditRows = list(filter(lambda r: r[0] == 'D' and r[1] == '7', importedRows))
            yield (filename, auditRows)

def removeDuplicatesFromPluData(pluRows):
    global pluCols
    pluRows.sort(key=lambda r: r[pluCols['name']])
    pluRows = list(filter(lambda r: bool(r[1][pluCols['name']] !=  pluRows[r[0]-1][pluCols['name']]) if r[0] else True, enumerate(pluRows)))
    return list(map(lambda r: r[1], pluRows))

def createPluStockCsvHeaders():
    return ['PluNo','StockCode','EcrText1','Price1']

def createPluStockCsvRow(auditRow):
    global auditCols
    barcode = str(auditRow[auditCols['barcode']]).lstrip('0')
    return [
        barcode,
        auditRow[auditCols['code']],
        auditRow[auditCols['name']],
        auditRow[auditCols['price']]
    ]

def createStockCsvHeaders():
    return ['StockCode', 'Description', 'Category', 'SupplierKey1', 'DUCost1']

def createStockCsvRow(stockRow):
    global stockCols, supplierData
    return [
        stockRow[stockCols['code']],
        stockRow[stockCols['name']],
        stockRow[stockCols['dept']],
        supplierData[stockRow[stockCols['supplier']]],
        stockRow[stockCols['cost']]
    ]

def importStockData(path):
    stockRows = importCsvData(path)
    print("starting Stock data length: {0}".format(len(stockRows)))
    stockData = list(map(createStockCsvRow, stockRows))
    stockData.insert(0, createStockCsvHeaders)
    return stockData

def importCategoryData(path):
    categoryRows = importCsvData(path)
    print("starting Category data length: {0}".format(len(categoryRows)))
    return list(map(lambda r: [r[0], r[1][0]], enumerate(categoryRows)))

def importPluData(path):
    global pluCols
    pluData = {}
    pluRows = importCsvData(path)
    print("Plu data length: {0}".format(len(pluRows)))
    for pluRow in pluRows:
        pluData[pluRow[pluCols['barcode']]] = pluRow
    return pluData

def importSupplierData(path):
    global supplierCols
    supplierData = {}
    supplierRows = importCsvData(path)
    print("Plu data length: {0}".format(len(supplierRows)))
    for supplierRow in supplierRows:
        supplierData[supplierRow[supplierCols['code']]] = supplierRow
    return supplierData

def importAuditData(dbPath, year):
    # pluData = {}
    # stockData = {}
    # rejectedRows = []
    global pluCols, stockCols, auditCols
    # stockRows = importCsvData(dbPath + '/Stock.cleaned.dat')
    # print("starting Stock data length: {0}".format(len(stockRows)))
    # for stockRow in stockRows:
    #     stockData[stockRow[stockCols['code']]] = stockRow
    # pluRows = importCsvData(dbPath + '/plu.csv')
    # pluRows = removeDuplicatesFromPluData(pluRows)
    # for pluRow in pluRows:
    #     pluData[str(pluRow[pluCols['barcode']]).strip()] = pluRow
    # print("Plu data length: {0}".format(len(pluRows)))
    pluStockData = []
    auditRowCount = 0
    for (filename, auditRows) in importAuditFileData(dbPath, year):
        auditRowCount+= len(auditRows)
        for auditRow in auditRows:
            try:
                pluStockData.append(createPluStockCsvRow(auditRow))
            # if stockData.get(auditRow[auditCols['code']], None) == None:
            #     if pluData.get(barcode, None) != None:
            #         stockData[auditRow[auditCols['code']]] = createRow(auditRow, pluData, barcode)
            #     else:
            #         nameMatchRows = list(filter(lambda r: r[pluCols['name']] == auditRow[auditCols['name']], pluRows))
            #         if len(nameMatchRows) > 0:
            #             print(nameMatchRows[0])
            #             for nameMatchRow in nameMatchRows:
            #                 stockData[auditRow[auditCols['code']]] = createRow(auditRow, pluData, nameMatchRow[pluCols['barcode']])
            #         else:
            #             rejectedRows.append(auditRow)
            except:
                print('Failed in file ' + filename + 'with data: ')
                print(auditRow)

        pluStockData.insert(0, createPluStockCsvHeaders)

    print("Audited Rows Count: {0}".format(auditRowCount))
    # print("Ending Stock data length: {0}".format(len(stockData)))
    # print("Rejected Rows length: {0}".format(len(rejectedRows)))

    # stockRows = []
    # for code in stockData:
    #     stockRows.append(stockData[code])

    # writeFile('Stock.new.csv', stockRows)

    # writeFile('Stock.rejected.csv', rejectedRows)



if __name__ == '__main__':
    supplierPath = str(input('Path to Supply.dat: '))
    supplierData = importSupplierData(supplierPath)

    catPath = str(input('Path to category.dat: '))
    categoryData = importCategoryData(catPath)

    stockPath = str(input('Path to Stock.dat: '))
    stockData = importStockData(stockPath)

    pluPath = str(input('Path to Plu.csv: '))
    pluData = importPluData(pluPath)

    auditDirPath = str(input('Path to Sp001/Audit directory: '))
    oldestYear = str(input('Oldest year to search audit files from (YYYY): '))
    pluStockData = importAuditData(auditDirPath, oldestYear)
    writeFile('pluStockData.csv', pluStockData)
