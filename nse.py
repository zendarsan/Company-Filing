import re
from datetime import datetime, timedelta
import fiscalyear
#Global Stuff make change
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import pickle
import magic
import logging
import zipfile
import io
import glob

class Director:
    def __init__(self, *initial_data, **kwargs):
        for dictionary in initial_data:
            for key in dictionary:
                date_check = re.search("\d\d-\w\w\w-\d\d\d\d", dictionary[key])
                if date_check:
                    setattr(self, key, datetime.strptime(dictionary[key], "%d-%b-%Y").date())
                else:
                    setattr(self, key, dictionary[key])
        for key in kwargs:
            date_check = re.search("\d\d-\w\w\w-\d\d\d\d", kwargs[key])
            if date_check:
                setattr(self, key, datetime.strptime(kwargs[key], "%d-%b-%Y").date())
            else:
                setattr(self, key, kwargs[key])
        self.unid = self.directorName.lower()+str(self.dateOfAppointment)
    def __eq__(self, item) -> bool:
        if isinstance(item, str):
            return item==self.directorName
        else:
            return isinstance(item, Director) and item.directorName==self.directorName
    
    def __str__(self):
        return f"{self.directorName}, Appointed {self.dateOfAppointment}, Cessation {self.dateOfCessation}, Rep {self.dateOfReport}"
    
    def __repr__(self):
        return f"{self.directorName}, Appointed {self.dateOfAppointment}, Cessation {self.dateOfCessation}, Rep {self.dateOfReport}"


def get_fin_year(month='MAR', year=2022):
    year = int(year)
    if month in ['APR','MAY', 'JUN', 'JUL', 'AUG', 'SEP','OCT','NOV', 'DEC']:
        end_year = year+1
    else:
        end_year = year
    return end_year

headers = {
    'authority': 'www.nseindia.com',
    'accept': '*/*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    # 'cookie': 'defaultLang=en; nseQuoteSymbols=[{"symbol":"MODIRUBBER","identifier":null,"type":"equity"}]; nsit=erzXHTxUeIRe2CmQ6CZFNC11; AKA_A2=A; bm_mi=68F3E1301ABC100ACA0B46DCEC43B6BD~YAAQJPV0aIXsWdqFAQAASu5C2xKWnw1joqHi7ckTZRCCrV+orR5E6ec0wdyiJM+R1zfdjhPcckn8u8xzU40SVCmPylvoM6Y6hG2Z4sF+ZNHIcP5y9i8yL5ZxTkLeo9aO+anjR83fX9uwEWB3MEDULt44LaQfZ+8oyNunwK0x7SEGJD2fpEv7aqXNNbcicVO7QWCRcHykGNKblG7QZ2Kd9P+lvtJt3t8on87+rXqeYM1mVXL80ET8VYrhnPpawF12ghngU+j9XF3fkncB210VkcuhnOkwZKWF5VCB5qo6wz8bFc7OTUKniqpVg34Tco92tS5JabRT4HiEMtnxbsNg5wA=~1; bm_sv=E7B9DAE45B6F0D6845E1FEF6C97BC070~YAAQJPV0aL/uWdqFAQAA6RhD2xIZTw1TnRczEu3IrUVOHGwHjMxr0gjuecxgC0Qx0Y3/TmwpP9efmXo1Rh1NupAy8d6PawLm1pTGOBkX063hJAsHXv81uDsZrL+6ktPP7+ghJYiFWiaWaNIG2WyFt4cXDH7SiQ2DdQMy8T1LOlVZToFdkL7++bRjbS3/vYtHc2Rd9Vx0+FrqeZExsfXUPDOGeV1biItmNwhaV6IEdF2j9kLMF4kgK+Nph3WP818RBszF~1; ak_bmsc=B8E52AD7311B19FB85A3035ABEE6ADBC~000000000000000000000000000000~YAAQJPV0aNDwWdqFAQAA6kVD2xK1TosC84WD2Ot4rJNmLpZbAdHcg57EdfV1WgsoISfU3PdCRyRvmBCS7S8399l44wRPmsOLkB7AMPdIcq3tMJcwLucVib5HJenThUHgTVxfQpRRvXX94/27M4B+7JBFFby7UEKOTx/6D0sxF6htttswZe9nOrJvak1CPWM8mhP+dBSzFCdb5l1MI7phhYOI9fMpdhLTv+5PtDD80ChoDsoI7R0+tseP1E2BJw1J9Q+5uQLmm8ghUQ9SYuP88kMlCEGfwNPxsg+RUs4TjznSwgyExHBAniQNnj/mkT1gun4glbl7di8KzgfBhk6VqcdUXkYJNZyM7ZbbtASjSCRSlYfnT2leN6HVZuWAIoWtKzsal1FWmu8bZnL8OfeCShOBmX4ruBj1LemkTiQoVGvy/oTlK1/FhqUIvg2/pI5FzsmT; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTY3NDQyMjU2NiwiZXhwIjoxNjc0NDI2MTY2fQ.BBDuiKxhozMRsJstQS5SQfgPrzHQLHAlOS0sp354FNc',
    'referer': 'https://www.nseindia.com/companies-listing/corporate-filings-governance?symbol=MODIRUBBER&tabIndex=equity',
    'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
}

if __name__=='__main__':
    fiscalyear.setup_fiscal_calendar(start_month=4)
    s = requests.Session()
    s.get("https://www.nseindia.com/companies-listing/corporate-filings-governance?symbol=ICICBANK&tabIndex=equity", headers=headers, timeout=5)
    
    if glob.glob("companies.pickle"):
        with open('companies.pickle', 'rb') as f:
            companies_data = pickle.load(f)
    else:
        symbols = pd.read_csv("companies.csv")
        companies_data = {}
        for symbol in symbols['SYMBOL']:
            if symbol in companies_data:
                continue
            params = {
            'symbol': symbol,
            'section': 'corp_info',
            }
            try:
                response = s.get('https://www.nseindia.com/api/quote-equity', params=params,  headers=headers, timeout=10)
            except requests.ReadTimeout: 
                print("ERROR", symbol)
                continue
            companies_data[symbol] = json.loads(response.content)
        with open('companies.pickle', 'wb') as f:
            pickle.dump(companies_data, f)

    companies = pd.read_csv('shareholding.csv')
    companies['pattern'] = 'Concentrated'
    cols = companies.columns
    companies.loc[companies[companies[cols[1]]==0].index, 'pattern'] = 'Fully Dispersed'
    companies.loc[companies[(companies[cols[1]]>0)&(companies[cols[1]]<25)].index, 'pattern'] = 'Partly Dispersed'
    companies.loc[companies[(companies[cols[1]]>=25)].index, 'pattern'] = 'Concentrated'

    for company in companies_data:
        print("Starting Company ", company)
        report_list = s.get('https://www.nseindia.com/api/corporate-governance-master', params={'index': 'equities','symbol': company,}, headers=headers, timeout=15) #Gets list of reports to pull RecID
        report_list = json.loads(report_list.content)
        qtr_reports = {}
        for qtr in report_list['data']:#Loops each quarter in the report list
            try:
                response = s.get('https://www.nseindia.com/api/corporate-governance', params={'recId': qtr['recordId']}, headers=headers, timeout=15)
            except requests.ReadTimeout:
                print("TimeOut", qtr)
            board = json.loads(response.content)
            comp = board['cobod'][0]['data']['CompositionBOD']
            qtr_reports[qtr['date']] = comp

        last_annual_report_year = int(companies_data[company]['corporate']['annualReport'][0]['toYr'])
        sorted_years = sorted(qtr_reports.keys())
        resignations = []
        for year in sorted_years:
            for raw_director in qtr_reports[year]:
                try:
                    director = Director(raw_director, dateOfReport=year)
                except:
                    print("DIRECTOR ERROR")
                    continue
                if director.dateOfCessation=='-' or director.dateOfAppointment=='-':
                    continue
                elif 'Independent' in director.category:
                    if director.dateOfCessation.year>last_annual_report_year or director.dateOfReport < director.dateOfCessation: #GovReport published after annual report, so skip. 
                        continue
                    if director.dateOfCessation.month in [3,4,6,7,9,10,12,1] and director.dateOfCessation.day in [31,30,28,1,2]:
                        continue
                    if abs((director.dateOfCessation - director.dateOfReport).total_seconds()) < timedelta(days=5).total_seconds():
                        continue
                    
                    appointment_cessation_diff = abs((director.dateOfAppointment - director.dateOfCessation).days/365.25)
                    rounded = round(appointment_cessation_diff)
                    if abs(appointment_cessation_diff - rounded) > 0.05:
                        print("Likely resignation ", director)
                        resignations.append(director)
                    else:
                        print("No Resig", abs(appointment_cessation_diff - rounded), director)
                    
                        
        years = {fiscalyear.FiscalDate(x.dateOfReport.year, x.dateOfReport.month, x.dateOfReport.day).fiscal_year for x in resignations}
        reports = []
        for year in years:
            report = [x for x in companies_data[company]['corporate']['annualReport'] if x['toYr']==str(year)]
            if report:
                reports.append(report[0])
                
        for report in reports:
            download = s.get(report['fileName'], stream=True, headers=headers)
            file_type = magic.from_buffer(download.content)
            if 'PDF' in file_type:
                with open(f"reports/{report['companyName']} {report['fromYr']}-{report['toYr']}.pdf", 'wb') as f:
                    f.write(download.content)
            elif 'Zip' in file_type:
                with zipfile.ZipFile(io.BytesIO(download.content)) as zip_ref:
                    zipinfos = zip_ref.infolist()
                    for i, zipinfo in enumerate(zipinfos):
                        zipinfo.filename = f"reports/{report['companyName']} {report['fromYr']}-{report['toYr']}.pdf"
                        zip_ref.extract(zipinfo, 'reports')
                        break

        companies.loc[companies[companies['COMPANY']==report_list['data'][0]['name']].index, 'Total Resignation'] = len(resignations)
        companies.loc[companies[companies['COMPANY']==report_list['data'][0]['name']].index, '2018 Resignations'] = ", ".join([x.directorName for x in resignations if x.dateOfReport.year==2018])
        companies.loc[companies[companies['COMPANY']==report_list['data'][0]['name']].index, '2019 Resignations'] = ", ".join([x.directorName for x in resignations if x.dateOfReport.year==2019])
        companies.loc[companies[companies['COMPANY']==report_list['data'][0]['name']].index, '2020 Resignations'] = ", ".join([x.directorName for x in resignations if x.dateOfReport.year==2020])
        companies.loc[companies[companies['COMPANY']==report_list['data'][0]['name']].index, '2021 Resignations'] = ", ".join([x.directorName for x in resignations if x.dateOfReport.year==2021])
        companies.loc[companies[companies['COMPANY']==report_list['data'][0]['name']].index, '2022 Resignations'] = ", ".join([x.directorName for x in resignations if x.dateOfReport.year==2022])
    companies.to_excel("Director Resignations")