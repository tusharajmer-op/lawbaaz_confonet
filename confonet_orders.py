import requests 
from bs4 import BeautifulSoup
from twocaptcha import TwoCaptcha
import config as cfg
import urllib3
import json
import psycopg2
from weasyprint import HTML
from datetime import datetime
from s3 import s3
from database import db_operations
sr = s3()
url = 'https://cms.nic.in/ncdrcusersWeb/search.do?method=loadSearchPub'
cookies = ''
headers = {
        'accept': 'text/html, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/html; charset=utf-8',
        'referer': 'https://cms.nic.in/ncdrcusersWeb/search.do?method=loadSearchPub',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
    }
headers2 = {
        'accept': 'text/html, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'Content-Type': 'application/x-www-form-urlencoded',
        'referer': 'https://cms.nic.in/ncdrcusersWeb/search.do?method=loadSearchPub',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
    }

def upload_to_db(data):
    query = f"""INSERT INTO lb_confonet_orders (
    unique_id,
    party_name,
    case_title,
    advocate_name,
    filing_date,
    disposal_date,
    date_of_upload,
    pdf,
    state,
    district
) VALUES (
    '{data['State']}/{data['District']}/{data['Case No.']}',
    '<pet>{data["Complainant"]} <res> {data["Respondent"]}',
    '{data["Complainant"]} vs {data["Respondent"]}',
    '<pet>{data["Complainant Advocate"]}<res> {data["Respondent Advocate"]}',
    '{data['Date of Filing']}',
    '{data['Date of Disposal']}',
    '{data['Date of Upload']}',
    '{data['pdf']}',
    '{data['State']}',
    '{data['District']}'
);"""
    print(query)
    res,data = db_operations(query,False)
    if res:
        print("Data inserted successfully")
    else:
        print("Error while inserting data")
        print(data)


    
    
def scrap_pdfs(content,state,district):
    final_data = []
    # with open('test.html', 'r') as f:
    #     content = f.read()
    # with open('test.html', 'w') as f:
    #     f.write(content)
    soup = BeautifulSoup(content, 'html.parser')
    data_div = soup.find('table')
    rows = data_div.find_all('tr')
    theads = rows[0].find_all('th')
    theaders = [thead.text for thead in theads]
    pdf_url = "https://cms.nic.in/ncdrcusersWeb/servlet/search.GetJudgement"
    for row in rows[1:]:
        cells = row.find_all('td')
        if len(cells) > 0:
            pdf_link = cells[-1].find('a')
            on_click = pdf_link.get('onclick',None)
            if(on_click != None):
                details = on_click.split('getJudgement(')[1].split(')')[0].split(',')
                data = {"caseidin":details[0].replace('"',"").strip(),"dtofhearing":details[1].replace('"',"").strip(),"method":"GetJudgement"}
                # print(data)
                pdf_response = requests.post(pdf_url, headers=headers2, cookies=cookies,data=data, verify=False)
                if pdf_response.status_code == 200:
                    content = pdf_response.text
                    HTML(string = content).write_pdf(f'''pdfs/{state}_{district}_{details[0].replace("/","_").replace('"',"").strip()}_{datetime.now().strftime("%Y%m%d%H")}.pdf''')
                    sr.upload_file(file_name=f'''pdfs/{state}_{district}_{details[0].replace("/","_").replace('"',"").strip()}_{datetime.now().strftime("%Y%m%d%H")}.pdf''',object_name=f'''confo_pdfs/{cells[0].replace("/","_").replace('"',"").strip()}.pdf''')
            else :
                pdf_response = requests.get(f"https://cms.nic.in/ncdrcusersWeb/"+pdf_link['href'], headers=headers, cookies=cookies, verify=False)
                if pdf_response.status_code == 200:
                    content = pdf_response.content
                    with open(f'''pdfs/{state}_{district}_{cells[0].text.replace("/","_").replace('"',"").strip()}_{datetime.now().strftime("%Y%m%d%H")}.pdf''', 'wb') as f:
                        f.write(content)
                    sr.upload_file(file_name=f'''pdfs/{state}_{district}_{cells[0].text.replace("/","_").replace('"',"").strip()}_{datetime.now().strftime("%Y%m%d%H")}.pdf''', object_name=f'''confo_pdfs/{cells[0].text.replace("/","_").replace('"',"").strip()}.pdf''')
            
            
        row_data = [cell.text for cell in cells]
        case_single_data = dict(zip(theaders,row_data))
        case_single_data['pdf'] = f'''confo_pdfs/{state}_{district}_{cells[0].text.replace("/","_").replace('"',"").strip()}.pdf'''
        case_single_data['State'] = state
        case_single_data['District'] = district
        upload_to_db(case_single_data)
        final_data.append(case_single_data)
    return final_data

def get_request_parameters():
    urllib3.disable_warnings()
    r = requests.get(url, verify=False)
    cookies = r.cookies.get_dict()
    soup = BeautifulSoup(r.text, 'html.parser')
    return cookies

def get_captcha_text():
    try:
        solver = TwoCaptcha(cfg.API_KEY)
        result = solver.normal('captcha.jpg')
        return result['code']
    except Exception as e:
        print(e)
        return None
def get_orders(stid,did,stdate,enddate,name,captcha_text,start=1):
    payload = {
        'method': 'GetHtml',
        'method': 'GetHtml',
        'stid': stid,
        'did': did,
        'stdate': stdate,
        'enddate': enddate,
        'par1': name,
        'fmt': 'T',
        'searchOpt': 'jud',
        'filterBy': 'on',
        'dateByPar': 'dtod',
        'start': start,
        'jsrc': 'FULL',
        'searchBy': '6',
        'captcha': captcha_text
    }
    url = "https://cms.nic.in/ncdrcusersWeb/servlet/search.GetHtml"
    response = requests.get(url, headers=headers, cookies=cookies, params=payload, verify=False)
    if response.status_code != 200:
        return "Error"
    content = response.text
    if(start == 1):
        pagination_soup = BeautifulSoup(content, 'html.parser')
        pagination = pagination_soup.find_all('td', {'class':'rhead'})
        page_count = pagination[-1].find_all('a')[-1]
        if page_count.text == 'Next':
            page_count = pagination[-2].find_all('a')[-1]['href'].split('start=')[1].split('&')[0]
        else :
            page_count = pagination[-1].find_all('a')[-1]['href'].split('start=')[1].split('&')[0]
        return scrap_pdfs(content,state=stid,district=did),page_count,start
    return scrap_pdfs(content,state=stid,district=did)
    
    

if __name__ == "__main__":
    stid = '13'
    did = '112' #pali,
    stdate = '01/01/2024'
    enddate = '16/08/2024'
    

    for i in ['122','112','114','111','105','124','110','100','103','99','120','123']:
        data = []
        start = 1
        page_count = 5
        session = requests.Session()    
        cookies = get_request_parameters()
        with open('captcha.jpg', 'wb') as f:
            f.write(session.get('https://cms.nic.in/ncdrcusersWeb/Captcha.jpg',headers=headers,cookies=cookies,verify=False).content)
        captcha_text = get_captcha_text()
        while start < int(page_count)+1:
            if start == 1:
                res_data,page_count,start = get_orders(stid,i,stdate,enddate,'NotApp',captcha_text)
            else : 
                res_data =  get_orders(stid,i,stdate,enddate,'NotApp',captcha_text,start)
            data = data + res_data
            print(start)
            with open('data.json', 'w') as f:
                f.write(json.dumps(data))
            start = start + 1
    # scrap_pdfs("")
    

    
    