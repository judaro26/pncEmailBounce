import requests
import json
import pandas as pd
import os
from datetime import datetime, timedelta, date
import datetime
import smtplib, ssl
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import io
import csv




# Define a custom function to serialize datetime objects
def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")
  
# Create a datetime object
dt = datetime.datetime.now()
currentime = dt - timedelta(seconds=1)
minustime=dt-timedelta(hours=24)


# Serialize the object using the custom function
currentjson_time = json.dumps(currentime, default=serialize_datetime)
replacementStr = 'Z'
newcurrenttimevalue = currentjson_time[:-1] + replacementStr
currentstrippedvalue=newcurrenttimevalue.replace('"','')

pastjson_time=json.dumps(minustime,default=serialize_datetime)
pastimevalue=pastjson_time[:-1] +replacementStr
paststrippedvalue=pastimevalue.replace('"','')


## LOCAL PATHS FOR GITHUB ACTIONS
firstReportcsv=os.path.join(os.getcwd(), 'firstReport.csv')
firstTextDoc=os.path.join(os.getcwd(), 'firstTextDoc.txt')
today = str(date.today())
final_reportName="PNC Bounced Email Report For " +today+".csv"


def getdocumentname():
    cookies = {
        'intercom-device-id-sfx5kqkh': '21d1a29f-d462-40d3-a40c-871500973816',
        '_gcl_au': '1.1.953156005.1695748000',
        '_mkto_trk': 'id:208-TJD-841&token:_mch-observeinc.com-1695748000268-73073',
        '_fbp': 'fb.1.1695748001141.263707556',
        '_ga': 'GA1.1.857720426.1694183932',
        '_ga_WHNXWVEKF7': 'GS1.1.1696001093.2.1.1696001093.60.0.0',
        '_hjSessionUser_3589698': 'eyJpZCI6IjI4YzkzZmM3LWZmZGUtNWIxYi05Nzc4LWFjZDc5NjE4YzMyMCIsImNyZWF0ZWQiOjE2OTYwMDEwOTUyNDUsImV4aXN0aW5nIjpmYWxzZX0=',
        'observe-customers': '%7B%22customerIds%22%3A%5B%22158395772829%22%5D%7D',
        'observe-session': 'eyJwYXNzcG9ydCI6eyJ1c2VyIjp7InVzZXJJZHMiOiJ7XCIxNTgzOTU3NzI4MjlcIjpcIjE5OTY0XCJ9IiwidG9rZW4iOiJxbk55QlFTYk1ha1pGOGh2czhzUEVwTUUzUHdyYnVoVCJ9fX0=',
        'observe-session.sig': 'u4wjsrXESX_ibS9E8V3p3mCF3D0',
        'intercom-session-sfx5kqkh': 'NWNJSm1rRVRvR0F3MWR1UU9McXVnN0tlS1FMMWJHazIvZG90RkEyQ254UExrbmc0ZWg3SXJDcXlpRnZQZ1Rvei0tb2REaG9JWXMzWFhjUEowRW4ycGtvQT09--567b92341def353bfd90b734d1f027f27739dd3d',
    }
    headers = {
        'authority': '158395772829.observeinc.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'authorization': 'Bearer 158395772829 qnNyBQSbMakZF8hvs8sPEpME3PwrbuhT',
        'content-type': 'application/json',
        # 'cookie': 'intercom-device-id-sfx5kqkh=21d1a29f-d462-40d3-a40c-871500973816; _gcl_au=1.1.953156005.1695748000; _mkto_trk=id:208-TJD-841&token:_mch-observeinc.com-1695748000268-73073; _fbp=fb.1.1695748001141.263707556; _ga=GA1.1.857720426.1694183932; _ga_WHNXWVEKF7=GS1.1.1696001093.2.1.1696001093.60.0.0; _hjSessionUser_3589698=eyJpZCI6IjI4YzkzZmM3LWZmZGUtNWIxYi05Nzc4LWFjZDc5NjE4YzMyMCIsImNyZWF0ZWQiOjE2OTYwMDEwOTUyNDUsImV4aXN0aW5nIjpmYWxzZX0=; observe-customers=%7B%22customerIds%22%3A%5B%22158395772829%22%5D%7D; observe-session=eyJwYXNzcG9ydCI6eyJ1c2VyIjp7InVzZXJJZHMiOiJ7XCIxNTgzOTU3NzI4MjlcIjpcIjE5OTY0XCJ9IiwidG9rZW4iOiJxbk55QlFTYk1ha1pGOGh2czhzUEVwTUUzUHdyYnVoVCJ9fX0=; observe-session.sig=u4wjsrXESX_ibS9E8V3p3mCF3D0; intercom-session-sfx5kqkh=NWNJSm1rRVRvR0F3MWR1UU9McXVnN0tlS1FMMWJHazIvZG90RkEyQ254UExrbmc0ZWg3SXJDcXlpRnZQZ1Rvei0tb2REaG9JWXMzWFhjUEowRW4ycGtvQT09--567b92341def353bfd90b734d1f027f27739dd3d',
        'origin': 'https://158395772829.observeinc.com',
        'referer': 'https://158395772829.observeinc.com/workspace/41239083/worksheet/PNC-BOUNCED-EMAIL-REPORT-41295443?time-preset=PAST_24_HOURS&time-start=2023-10-08T08.27.35-07.00&time-end=2023-10-09T08.27.35-07.00&s=19964-gdcgdp9c',
        'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'traceparent': '00-6cdcd91b20829afc6faf56c0b500e850-0b7d2aac3db0256a-01',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    }
    params = {
        'queryName': 'ExportQuery',
    }
    json_data = {
        'operationName': 'ExportQuery',
        'variables': {
            'query': {
                'stages': [
                    {
                        'id': 'stage-wkeo28hn',
                        'input': [
                            {
                                'inputName': 'Blend/Logs/Restricted/AWS SES Logs',
                                'datasetId': '41267537',
                                'inputRole': 'Data',
                            },
                        ],
                        'layout': {},
                        'pipeline': 'filter not is_null(data_json)\nmake_col message_timestamp:string(data_json.mail.timestamp)\nmake_col to:data_json.mail.commonHeaders.to\nmake_col to: (string(to[0]))\nmake_col replyTo:data_json.mail.commonHeaders.replyTo\nmake_col replyTo: (string(replyTo[0]))\nmake_col subject:string(data_json.mail.commonHeaders.subject)\nmake_col notificationType:string(data_json.notificationType)\nmake_col bounceType:string(data_json.bounce.bounceSubType)\nfilter replyTo~pnc\nfilter data_json~bounce\n\n\npick_col\n    timestamp,\n    message_timestamp,\n    to,\n    bounceType,\n    notificationType,\n    replyTo,\n    subject\n',
                    },
                ],
                'outputStage': 'stage-wkeo28hn',
                'layout': {},
                'parameterValues': [],
            },
            'params': {
                'startTime': '2023-10-08T15:27:35.197Z',
                'endTime': '2023-10-09T15:27:35.197Z',
            },
            'presentation': {
                'limit': '100000',
                'linkify': True,
            },
            'filename': 'PNC-BOUNCED-EMAIL-REPORT-1696867820070.csv',
            'exportFormat': 'Csv',
        },
        'query': 'query ExportQuery($query: MultiStageQueryInput!, $params: QueryParams!, $presentation: StagePresentationInput!, $filename: String, $exportFormat: ExportFileFormat) {\n  exportQuery(\n    query: $query\n    params: $params\n    presentation: $presentation\n    filename: $filename\n    exportFormat: $exportFormat\n  ) {\n    exportUrl\n    exportUrlExpiration\n    exportFilename\n    exportFormat\n    __typename\n  }\n}',
    }
    response = requests.post(
        'https://158395772829.observeinc.com/v1/meta',
        params=params,
        cookies=cookies,
        headers=headers,
        json=json_data,
    )
    responsejson=json.loads(response.text)
    global csvreporturl
    csvreporturl=responsejson['data']['exportQuery']['exportUrl']
    csvurlcall=requests.get(csvreporturl)
    f=open(firstTextDoc,'w')
    f.write(csvurlcall.text)
    ### OPEN TEXT DOCUMENT AND TRANSFORM TO
    r=open(firstTextDoc, "r")
    responsefromtext = r.read()
    responsefromtext = responsefromtext.replace('",', ',')
    responsefromtext=responsefromtext.replace('"','')
    f=open(firstTextDoc,'w')
    f.write(responsefromtext)
    df=pd.read_csv(firstTextDoc)
    df.to_csv(firstReportcsv,encoding='utf-8', index=None)
    print('FIRST REPORT CREATED SUCCESSFULLY.')



#### FUNCTION TO SEND EMAIL
def emailsenderfunction():
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login('judaro26@gmail.com', 'vlaeqdrusqvwpslm')
        today = str(date.today())
        str_io = io.StringIO()
        df = pd.read_csv(firstReportcsv)
        df.to_html(buf=str_io)
        table_html = str_io.getvalue()
        html="""\
        <html>
        <body>
        <div>
        <div>Hi there,</div>
        <div>&nbsp;</div>
        <div>We have identified an issue with a document not successfully exporting to Empower.</div>
        <div>&nbsp;</div>
        <div>We are including a list of potentially affected documents for your review.</div>
        <div>&nbsp;</div>
        <div>
            <p>{table_html}</p>
        <div>
        <div>
        <div>All the best,</div>
        <div>&nbsp;</div>
        <div>Blend Support</div>
        </div>
        </div>
        </div>
        </body>
        </html>
        """.format(table_html=table_html)
        try:
            msg = EmailMessage()
            msg.set_content('simple text would go here - This is a fallback for html content')
            msg.add_alternative(html, subtype='html')
            msg['Subject'] = ('PNC Bounced Email Report '+today)
            msg['From'] = 'my_email'
            msg['To'] = 'victoria-reyna@blend.com','connie@blend.com','juan-rodriguez@blend.com'
            msg['Cc'] = ''
            msg['Bcc'] = ''
            with open (firstReportcsv,'rb') as f:
                file_data=f.read()
            msg.add_attachment(file_data, maintype="application", subtype="csv", filename=final_reportName)
            smtp.send_message(msg)
            print("SUCCESSFULLY SENT EMAIL!")
        except:
            print("Something went wrong!!!")

getdocumentname()
emailsenderfunction()
