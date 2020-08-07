import json
import math
import pandas as pd
from datetime import datetime
import QuantLib as ql
import numpy as np
from utils import SABRLib

def py2ql_date(pydate):
    
    return ql.Date(pydate.day,pydate.month,pydate.year)

def getYearFrac(date0, date1):

    day_count=ql.Actual365Fixed()
    yrs = day_count.yearFraction(py2ql_date(date0), py2ql_date(date1))
    
    if yrs==0:
        yrs+=0.001
        
    return yrs

def jsonPrint(d):
    print(json.dumps(d, indent=2))
    
def printList(l):
    for item in l:
        print(item)
    
def getATMstk(F0,Klist):
    
    K0=Klist[0]
    Kdis=abs(K0-F0)
    K0pos=0
    
    for i in range(0,len(Klist)):
        if abs(Klist[i]-F0)<Kdis:
            K0pos=i
            K0=Klist[i]
            Kdis=abs(Klist[i]-F0)
            
    return K0

def volRealized(pxList, annu_factor=365):
    
    retList=[]
    for i in range(1,len(pxList)):
        
        retList.append(pxList[i]/pxList[i-1]-1)
        
    return np.std(retList)*math.sqrt(annu_factor)*100

def getOptionDataFromDeribit():
    pass
    
  
def time_tango(date):
        return datetime.strptime("{}".format(date), "%d%b%y")
        #return datetime.strptime("{}".format(dates), "%Y%m%d")

def time_to_expire(date):
        return datetime.strptime("{}".format(date+" 16:00:00"), "%d%b%y %H:%M:%S")


def generateVols(x, y, qVol):
    y_axis=[]
    for j in range(0,len(y)):
        x_axis=[]
        for i in range(0,len(x)):
            x_axis.append(100*SABRLib.get_vol(qVol, x[i], y[j]))
            
        y_axis.append(x_axis)
            
    return np.array(y_axis)