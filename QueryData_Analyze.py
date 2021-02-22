# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 10:43:36 2021

@author: kunter.kunt
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jan 18 10:22:40 2021

@author: kunter.kunt
"""

import os
import pandas as pd
import numpy as np
from itertools import repeat
import psycopg2
import time 
from datetime import date, timedelta
from datetime import datetime 




def main():
    
    today = date.today()
    yesterday = today - timedelta(days = 1)
    yesterday=yesterday.strftime('%Y-%m-%d')
    daysago=today - timedelta(days = 8)
    daysago=daysago.strftime('%Y-%m-%d')
    today=today.strftime('%Y-%m-%d')
    
    
    dateStr="'{daysago}' and '{yesterday}'".format(daysago=daysago,yesterday=yesterday)
    queryStr='select "RptDate","AdtTitle",sum("RptRevenue") as Revenue from dbo."AdReport" join dbo."AdTag" on "RptAdtId" = "AdtId" where "RptDate" between {dateStr} group by "RptDate","AdtTitle"'.format(dateStr=dateStr)
    
    connection=psycopg2.connect(database='HeaderLiftDB',user=***,password=***,host='35.205.135.216')
    dfquery=pd.read_sql_query(queryStr,connection)
    connection.close()
    
    z=dfquery.groupby(['AdtTitle']).apply(lambda x:x.sort_values(['RptDate'])).reset_index(drop=True)##asıl df bu
    
    df2=z.rename(columns={'RptDate':'date','AdtTitle':'adunit','revenue':'total Revenue'})
    
    dfadunit_new_monday=df2
        
    
    
    
    
    listadunits_monday=[]##df'deki unique ad unitler
    for i in dfadunit_new_monday['adunit'].unique():
        listadunits_monday.append(i)
        
    date0=dfadunit_new_monday['date'][0]
    date1=dfadunit_new_monday['date'][1]
    date2=dfadunit_new_monday['date'][2]
    date3=dfadunit_new_monday['date'][3]
    date4=dfadunit_new_monday['date'][4]
    date5=dfadunit_new_monday['date'][5]
    date6=dfadunit_new_monday['date'][6] 
    date_final=dfadunit_new_monday['date'][7]  
    
    
    listadunits_monday_HighRev=[]
    for i in listadunits_monday:
        tempdf=dfadunit_new_monday[dfadunit_new_monday['adunit']==i]
        if tempdf['total Revenue'].values.mean()>10.0:
            listadunits_monday_HighRev.append(i)
            
    dfhighRev_adUnits_monday= dfadunit_new_monday[dfadunit_new_monday['adunit'].isin(listadunits_monday_HighRev)] 
    ##üstteki sadece high rev adunitlerden oluşan data frame -ortalama totRev 10'un üstünde olanlar         
        
    
    
    
    listadunits_monday_HighRev_last=[]
    for i in listadunits_monday_HighRev:
        tempdf=dfhighRev_adUnits_monday[dfhighRev_adUnits_monday['adunit']==i]
        if len(tempdf)==8:
            listadunits_monday_HighRev_last.append(i)
            
    dfhighRev_adUnits_monday= dfadunit_new_monday[dfadunit_new_monday['adunit'].isin(listadunits_monday_HighRev_last)] 
    ##en son highRev df, hem mean rev 10 un üstünde hem de herbir adunitten 8 date de var 
    listdeneme=[]         
    listproblemAdUnits_monday=[]
    for i in listadunits_monday_HighRev_last:
        tempdf=dfhighRev_adUnits_monday[dfhighRev_adUnits_monday['adunit']==i]
        rev0=tempdf[tempdf['date'].isin([date0])]['total Revenue'].values[0]
        rev1=tempdf[tempdf['date'].isin([date1])]['total Revenue'].values[0]
        rev2=tempdf[tempdf['date'].isin([date2])]['total Revenue'].values[0]
        rev3=tempdf[tempdf['date'].isin([date3])]['total Revenue'].values[0]
        rev4=tempdf[tempdf['date'].isin([date4])]['total Revenue'].values[0]
        rev5=tempdf[tempdf['date'].isin([date5])]['total Revenue'].values[0]
        rev6=tempdf[tempdf['date'].isin([date6])]['total Revenue'].values[0]
        rev7=tempdf[tempdf['date'].isin([date_final])]['total Revenue'].values[0]
        
        minrev_6days=min(rev0,rev1,rev2,rev3,rev4,rev5)
        meanrev_6days=(rev0+rev1+rev2+rev3+rev4+rev5)/6
        meanrev_7days=(rev0+rev1+rev2+rev3+rev4+rev5+rev6)/7
        
        if meanrev_7days<15.0:
            if min(rev0,rev1,rev2,rev3,rev4,rev5,rev6)*0.60>rev7:
                listproblemAdUnits_monday.append(i)
        elif meanrev_7days<30.0:
            if min(rev0,rev1,rev2,rev3,rev4,rev5,rev6)*0.85>rev7:
                listproblemAdUnits_monday.append(i)
        elif meanrev_7days<40.0:
            if min(rev0,rev1,rev2,rev3,rev4,rev5,rev6)*0.9>rev7:
                listproblemAdUnits_monday.append(i)
                
            
        else:
            if rev7<meanrev_7days*0.80:
                listproblemAdUnits_monday.append(i)
                
    dfproblemAdUnits_monday=dfhighRev_adUnits_monday[dfhighRev_adUnits_monday['adunit'].isin(listproblemAdUnits_monday)]
    
    ##burdan sonra denemelerini yapacaksın
    #dfproblemAdUnits_monday problemli adunitlerin olduğu sorted olmayan bir df
    #listproblemAdUnits_monday'de problemli ad unitlerin olduğu liste
    
    dictadunits={}
    for i in listproblemAdUnits_monday:
        tempdf=dfproblemAdUnits_monday[dfproblemAdUnits_monday['adunit']==i]
        avg_first7=tempdf['total Revenue'][:7].mean()
        lastval=tempdf['total Revenue'][7:8].values[0]
        rate=lastval/avg_first7
        dictadunits[i]=rate
        
    dictadunitsSorted=sorted(dictadunits.items(), key=lambda x: x[1])
    
    listadunits_sortedRevLossRate=[]
    for i in dictadunitsSorted:
        listadunits_sortedRevLossRate.append(i[0])
        
    listtotRevSorted=[]
    for i in listadunits_sortedRevLossRate:
        t=dfproblemAdUnits_monday[dfproblemAdUnits_monday.adunit==i]
        for j in t['total Revenue']:
            listtotRevSorted.append(j)
            
    listdateSorted=[]
    for i in listadunits_sortedRevLossRate:
        t=dfproblemAdUnits_monday[dfproblemAdUnits_monday.adunit==i]
        for k in t['date']:
            listdateSorted.append(k)
            
    listadunits_sorted_repeated=[]
    for i in listadunits_sortedRevLossRate:
        listadunits_sorted_repeated.extend(repeat(i, 8))
        
    data_units_sorted={'adunit':listadunits_sorted_repeated,'date':listdateSorted,'total Revenue':listtotRevSorted}
    dfproblemAdUnits_sorted_last_wednesday=pd.DataFrame(data_units_sorted,columns=['adunit','date','total Revenue'])
        
    dfproblemAdUnits_sorted_last_wednesday.to_excel(today+'_output.xlsx')
            
    
    ##burda bitiyor denemen 
    #dfproblemAdUnits_sorted_last_wednesday revenue loss'una göre sorted olan ad unitler
        
    ################################################################################## 
    ################################################################################## 
    ################################################################################## 
    ################################################################################## 
    ################################################################################## 
    ################################################################################## 
    ################################################################################## 
    ##################################################################################   
 
if __name__ == "__main__":
    main()      
    


























 
    
#if __name__ == "__main__":
#    main()   
    
 
    
    
    
    
    
    
    
    
    












































##aşağısı datayı görmek açısından denemeler
#testlist=[]
#for i in listadunits_monday:
#    tempdf=dfadunit_new_monday[dfadunit_new_monday.adunit==i]
#    if len(tempdf)<3:
#        testlist.append(i)
#        
#testlist_only1=[]
#for i in listadunits_monday:
#    tempdf=dfadunit_new_monday[dfadunit_new_monday.adunit==i]
#    if len(tempdf)==1:
#        testlist_only1.append(i)
#        
#       
#testlist_only17=[]        
#for i in testlist_only1:
#    tempdf=dfadunit_new_monday[dfadunit_new_monday.adunit==i]
#    if len(tempdf[tempdf['date'].isin([date_final])])==1:
#        testlist_only17.append(i)
#        
#testlist4=[]
#for i in listadunits_monday:
#    tempdf=dfadunit_new_monday[dfadunit_new_monday.adunit==i]
#    if len(tempdf)==3:
#        if len(tempdf[tempdf['date'].isin([date_final,date6,date5])])==3:
#            testlist4.append(i)
            
##eğer 3 taneyse ve içinde 17 varsa 15-16-17 şeklinde            
##235 element testlist            
               
     
        
    
#testlist_onlyNot8=[]
#for i in listadunits_highRevMonday:
#    tempdf=dfhighRevMonday[dfhighRevMonday.adunit==i]
#    if len(tempdf[tempdf['date'].isin([date_final,date0,date1,date2,date3,date4,date5,date6])])!=8:
#        if len(tempdf[tempdf['date'].isin([date_final])])==1:
#            testlist_onlyNot8.append(i)
#        
            
##dfhighrevmonday'den sadece 17 olanları atabilirsin mesela            
        

#dfadunit_new_mondayy        
        

    
    
             
        
        
        
    
