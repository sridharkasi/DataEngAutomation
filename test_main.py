import openpyxl
from pyspark.sql import SparkSession
import ReusableMethods
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
global executionDict
global spark
# def test_main():
def test_ReadExecXl():
    # global spark
    filename="./Data/Execution.xlsx"
    wb = openpyxl.load_workbook(filename)
    ws = wb['Sheet1']
    rw = ws.max_row
    max_column=ws.max_column
    executionDict = dict()
    for i in range(2, rw + 1):
        for j in range(1, max_column+1 ):
            flag = 0
            execution = ws.cell(row=i, column=1)
            # print(execution.value)
            if (execution.value == 'Y'):
                Keyname = ws.cell(row=1, column=j)
                keyvalue= ws.cell(row=i, column=j)
                executionDict[Keyname.value]= keyvalue.value
                # odict['Policy' + str(i)] = i['PolNumber']
            # executecase(executionDict["Action"], executionDict["SourceFormat"])
            # executecase(executionDict["Action"], executionDict["SourceFormat"], executionDict["SourceFilePath"],executionDict["TargetFilePath"], executionDict["TargetFormat"], executionDict["SQL"])
            else:
                flag=1
                break

        if(flag==0):
            ReusableMethods.executecase(executionDict["Action"], executionDict["SourceFormat"], executionDict["SourceFilePath"], executionDict["TargetFilePath"], executionDict["TargetFormat"], executionDict["SQL"], executionDict["TestcaseName"])






