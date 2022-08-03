from email import header
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
import pandas as pd

a=requests.get('http://aws.imd.gov.in:8091/AWS/dataview.php?a=AWSAGRO&b=MAHARASHTRA&c=MUMBAI_CITY&d=MUMBAI_COLABA&e=2022-08-03&f=2022-08-03&g=ALL_HOUR&h=ALL_MINUTE')

soup=BeautifulSoup(a.content,'html.parser')

data=[]

y=soup.find_all("table")[0].find_all("tr")

for i in y:
    sub=[]
    for j in i:
        sub.append(j.get_text())
    data.append(sub)

for i in data[0]:
    if i=="\n":
        data[0].remove(i)

df=pd.DataFrame(data=data[1:],columns=data[0])

df.to_csv("IMD.csv")   

