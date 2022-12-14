import requests, xmltodict, json, time, random, re
from datetime import datetime
import numpy as np
import db_update

"""판례 크롤러"""
class precedent_crawler:
    def __init__(self, information_search, embezzlement_search):
        self.info_words = information_search
        self.emb_words = embezzlement_search
    
    """영업비밀유출"""
    def information_crawler(self):
        for word in self.info_words:
            print("판례" + "=" + word)
            final_info_information_tuple = {}
            refer_prec_list = []
            final_info_information_list = []
            information_return_list = []
            num = 1
            for i in range(1388):
                try:
                    url = "http://www.law.go.kr/DRF/lawSearch.do?OC=iklee0276&target=prec&type=XML"
                    params = {'query':word, 'display':100, 'page':num, 'search':2}
                    list_re = requests.get(url,params=params)
                    time.sleep(random.uniform(5,10))
                except:
                    pass
                
                if list_re.status_code != 200:
                    break
                num = num+1
                try:
                    lxml = xmltodict.parse(list_re.text)
                    jsonDump = json.dumps(lxml)
                    jsonBody = json.loads(jsonDump)
                    items = jsonBody['PrecSearch']['prec']
                except:
                    continue
                
                urls = []
                for item in items:
                    i = re.sub(r"HTML&mobileYn=", "XML", item['판례상세링크'])
                    urls.append("http://www.law.go.kr" + i)
                
                urls = list(set(urls))
                
                for url in urls:
                    try:
                        body = requests.get(url)
                        time.sleep(random.uniform(5,10))
                        
                        if body.status_code != 200:
                            break
                        
                        lxml = xmltodict.parse(body.text)
                        jsonDump = json.dumps(lxml)
                        jsonBody = json.loads(jsonDump)
                        
                        name = jsonBody['PrecService']['법원명'] + " " + jsonBody['PrecService']['선고일자'] + " " + jsonBody['PrecService']['선고'] + " " + jsonBody['PrecService']['사건번호'] + " " + jsonBody['PrecService']['판결유형']
                        description = jsonBody['PrecService']['판례내용']
                        created = datetime.strptime(jsonBody['PrecService']['선고일자'], '%Y%m%d').date()
                        
                        final_info_information_tuple['name'] = name
                        final_info_information_tuple['description'] = description
                        final_info_information_tuple['case_id'] = jsonBody['PrecService']['사건번호']
                        final_info_information_tuple['type'] = "prec"
                        final_info_information_tuple['created'] = created
                        final_info_information_tuple['prec_serial_num'] = jsonBody['PrecService']['판례정보일련번호']
                        final_info_information_tuple['case_name'] = jsonBody['PrecService']['사건명']
                        final_info_information_tuple['sentence'] = jsonBody['PrecService']['선고']
                        final_info_information_tuple['court_name'] = jsonBody['PrecService']['법원명']
                        final_info_information_tuple['court_code'] = jsonBody['PrecService']['법원종류코드']
                        final_info_information_tuple['case_type'] = jsonBody['PrecService']['사건종류명']
                        final_info_information_tuple['case_code'] = jsonBody['PrecService']['사건종류코드']
                        final_info_information_tuple['discriminant_type'] = jsonBody['PrecService']['판결유형']
                        final_info_information_tuple['holding'] = jsonBody['PrecService']['판시사항']
                        final_info_information_tuple['thrust'] = jsonBody['PrecService']['판결요지']
                        try:
                            final_info_information_tuple['refer_prec'] = re.sub('(<([^>]+)>)', '', jsonBody['PrecService']['참조판례'])
                        except:
                            final_info_information_tuple['refer_prec'] = jsonBody['PrecService']['참조판례']
                        final_info_information_tuple['refer_provision'] = jsonBody['PrecService']['참조조문']
                        final_info_information_tuple['category'] = "information_leak"
                        final_info_information_tuple['link'] = "https://www.law.go.kr/precInfoP.do?precSeq="+jsonBody['PrecService']['판례정보일련번호']

                        final_info_information_list.append(final_info_information_tuple)
                        final_info_information_list = list(map(dict, set(tuple(sorted(d.items())) for d in final_info_information_list)))
                        information_return_list.append(final_info_information_list)
                    except:
                        pass
                try:
                    refer_prec_list = list(map(dict, set(tuple(sorted(d.items())) for d in refer_prec_list)))
                    information_final_case = np.concatenate(information_return_list).tolist()
                    final_case = list(map(dict, set(tuple(sorted(i.items())) for i in information_final_case)))

                    db = db_update.update(final_case)
                    """크롤링 결과를 DB에 INSERT함"""
                    db.update_cases_db()
                    db.update_case_tag_db()
                    db.update_event_case_db()
                except:
                    pass
            
    """횡령"""
    def embezzlement_crawler(self):
        for word in self.emb_words:
            print("판례" + "=" + word)
            final_info_emb_tuple = {}
            final_info_emb_list = []
            refer_prec_list = []
            embezzlement_return_list = []
            num = 1
            for i in range(2500):
                try:
                    url = "http://www.law.go.kr/DRF/lawSearch.do?OC=iklee0276&target=prec&type=XML"
                    params = {'query':word, 'display':100, 'page':num, 'search':2}
                    list_re = requests.get(url,params=params)
                except:
                    pass
                
                if list_re.status_code != 200:
                    break
                num = num+1
                try:
                    lxml = xmltodict.parse(list_re.text)
                    jsonDump = json.dumps(lxml)
                    jsonBody = json.loads(jsonDump)
                    items = jsonBody['PrecSearch']['prec']
                except:
                    continue
                
                urls = []
                for item in items:
                    i = re.sub(r"HTML&mobileYn=", "XML", item['판례상세링크'])
                    urls.append("http://www.law.go.kr" + i)
                
                urls = list(set(urls))
                
                for url in urls:
                    try:
                        body = requests.get(url)
                        time.sleep(random.uniform(5,10))
                        
                        if body.status_code != 200:
                            break
                        
                        lxml = xmltodict.parse(body.text)
                        jsonDump = json.dumps(lxml)
                        jsonBody = json.loads(jsonDump)
                        
                        name = jsonBody['PrecService']['법원명'] + " " + jsonBody['PrecService']['선고일자'] + " " + jsonBody['PrecService']['선고'] + " " + jsonBody['PrecService']['사건번호'] + " " + jsonBody['PrecService']['판결유형']
                        description = jsonBody['PrecService']['판례내용']
                        created = datetime.strptime(jsonBody['PrecService']['선고일자'], '%Y%m%d').date()
                        
                        final_info_emb_tuple['name'] = name
                        final_info_emb_tuple['description'] = description
                        final_info_emb_tuple['case_id'] = jsonBody['PrecService']['사건번호']
                        final_info_emb_tuple['type'] = "prec"
                        final_info_emb_tuple['created'] = created
                        final_info_emb_tuple['prec_serial_num'] = jsonBody['PrecService']['판례정보일련번호']
                        final_info_emb_tuple['case_name'] = jsonBody['PrecService']['사건명']
                        final_info_emb_tuple['sentence'] = jsonBody['PrecService']['선고']
                        final_info_emb_tuple['court_name'] = jsonBody['PrecService']['법원명']
                        final_info_emb_tuple['court_code'] = jsonBody['PrecService']['법원종류코드']
                        final_info_emb_tuple['case_type'] = jsonBody['PrecService']['사건종류명']
                        final_info_emb_tuple['case_code'] = jsonBody['PrecService']['사건종류코드']
                        final_info_emb_tuple['discriminant_type'] = jsonBody['PrecService']['판결유형']
                        final_info_emb_tuple['holding'] = jsonBody['PrecService']['판시사항']
                        final_info_emb_tuple['thrust'] = jsonBody['PrecService']['판결요지']
                        try:
                            final_info_emb_tuple['refer_prec'] = re.sub('(<([^>]+)>)', '', jsonBody['PrecService']['참조판례'])
                        except:
                            final_info_emb_tuple['refer_prec'] = jsonBody['PrecService']['참조판례']
                        final_info_emb_tuple['refer_provision'] = jsonBody['PrecService']['참조조문']
                        final_info_emb_tuple['category'] = "embezzlement"
                        final_info_emb_tuple['link'] = "https://www.law.go.kr/precInfoP.do?precSeq="+jsonBody['PrecService']['판례정보일련번호']

                        final_info_emb_list.append(final_info_emb_tuple)
                        final_info_emb_list = list(map(dict, set(tuple(sorted(d.items())) for d in final_info_emb_list)))
                        embezzlement_return_list.append(final_info_emb_list)
                    except:
                        pass
                try:
                    embezzlement__final_case = np.concatenate(embezzlement_return_list).tolist()
                    final_case = list(map(dict, set(tuple(sorted(i.items())) for i in embezzlement__final_case)))
                    refer_prec_list = list(map(dict, set(tuple(sorted(d.items())) for d in refer_prec_list)))

                    db = db_update.update(final_case)
                    """크롤링 결과를 DB에 INSERT함"""
                    db.update_cases_db()
                    db.update_case_tag_db()
                    db.update_event_case_db()
                except:
                    pass