from Auth import Authentication
import csv
from polygon.rest import RESTClient
from sqlalchemy import create_engine 
from sqlalchemy import text
import pandas as pd
from math import sqrt
if __name__ == '__main__':
    outputFileName = 'output.csv'
    file = open(outputFileName, 'w')
    header = ['Key', 'Min', 'Max', 'Mean', "Vol", "FD"] 
    writer = csv.DictWriter(file, fieldnames=header)
    writer.writeheader()
    auth = Authentication()
    auth.getData(outputFileName)