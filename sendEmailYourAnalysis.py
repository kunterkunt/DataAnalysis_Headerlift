




#import win32api
#import pywintypes

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import time 
import os 
from pandas import DataFrame as df


import pandas as pd
import numpy as np
from itertools import repeat
import psycopg2
import time 
from datetime import date, timedelta
from datetime import datetime 

from datetime import date, timedelta
from datetime import datetime as dt



def main():
    
    today = date.today()
    today=today.strftime('%Y-%m-%d')
    
    
    
    
    
    
    
    
    
    email_user = 'k.kunt@azerion.com'
    email_password = ***
    email_send = 'm.tanyerli@azerion.com'
    
    #subject = today.strftime('%m-%d-%Y')
    subject = 'Problematic Ad units on '+today
    
    msg = MIMEMultipart()
    msg['From'] = email_user
    msg['To'] = email_send
    msg['Subject'] = subject
    
    body ='Ad Units which have largest revenue loss on '+today
    msg.attach(MIMEText(body,'plain'))
    
    filename=today+'_output.xlsx'
    attachment=open(filename,'rb')
    
    part = MIMEBase('application','octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition',"attachment; filename= "+filename)
    
    msg.attach(part)
    text = msg.as_string()
    server = smtplib.SMTP('smtp.gmail.com',587)
    #server = smtplib.SMTP_SSL('smtp.gmail.com',465)
    server.starttls()
    server.login(email_user,email_password)
    
    
    server.sendmail(email_user,email_send.split(','),text)
    server.quit()
    
if __name__ == "__main__":
    main() 
    
   







