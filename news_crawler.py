import requests, time, random
from datetime import datetime
import numpy as np
import difflib
import db_update
from newspaper import Article, Config

"""뉴스 기사 크롤러"""
class article_crawler:
    def __init__(self, information_search, embezzlement_search, client_id, client_secret):
        self.info_words = information_search
        self.emb_words = embezzlement_search
        self.client_id = client_id
        self.client_secret = client_secret
    
    """영업비밀유출"""
    def information_crawler(self):
        for word in self.info_words:
            print("뉴스" + "=" + word)
            final_info_information_tuple = {}
            final_info_information_list = []
            information_return_list = []
            start = 1
            for i in range(1388):
                final_urls = []

                try:
                    #html 불러오기
                    url = "https://openapi.naver.com/v1/search/news.json"
                    headers = {'X-Naver-Client-Id':self.client_id, 'X-Naver-Client-Secret':self.client_secret}
                    params = {'query':word, 'display':100, 'start':start, 'sort':'date'}
                    original_html = requests.get(url, headers=headers, params=params)
                    time.sleep(random.uniform(5,10))
                except:
                    break
                
                if original_html.status_code !=200:
                    break
                
                start += 100
                
                json_html = original_html.json()
                newslist = json_html['items']
                urls = []
                created_list = []
                for item in newslist:
                    if "news.naver.com" in item['link']:
                        dates = item['pubDate'].split(" ")
                        date = datetime.strptime(dates[3]+"-"+dates[2]+"-"+dates[1]+" "+dates[4], '%Y-%b-%d %H:%M:%S')
                        created_list.append(date)
                        urls.append(item['link'])
                
                final_urls = list(set(urls))
                
                for url, date in zip(final_urls, created_list):
                    try:
                        config = Config()
                        config.request_timeout = 10
                        
                        article = Article(url, language = 'ko', config=config)
                        time.sleep(random.uniform(5,10))
                        article.download()
                        article.parse()

                        NewsFeed = article.text

                        final_info_information_tuple['name'] = article.title
                        final_info_information_tuple['description'] = NewsFeed
                        final_info_information_tuple['link'] = url
                        final_info_information_tuple['created'] = date
                        final_info_information_tuple['type'] = "news"
                        final_info_information_tuple['category'] = "information_leak"

                        final_info_information_list.append(final_info_information_tuple)
                        final_info_information_list = list(map(dict, set(tuple(sorted(d.items())) for d in final_info_information_list)))
                        information_return_list.append(final_info_information_list)
                    except:
                        pass
                try:
                    information_final_case = np.concatenate(information_return_list).tolist()
                    final_case = list(map(dict, set(tuple(sorted(d.items())) for d in information_final_case)))

                    deld = final_case[:]
                    d = []
                    e = []
                    for i in final_case:
                        if not i in d:
                            if not i in e:
                                for j in deld:
                                    if not i == j:
                                        sm = difflib.SequenceMatcher(None, i['name'], j['name'])
                                        similar = sm.ratio()
                                        if similar > 0.25:
                                            d.append(j)
                                        else:
                                            pass
                                    else:
                                        pass
                                e.append(i)

                    final_case = [x for x in final_case if x not in d]
                    
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
            print("뉴스" + "=" + word)
            final_info_emb_tuple = {}
            final_info_emb_list = []
            embezzlement_return_list = []
            start = 1
            for i in range(2500):
                final_urls = []

                try:
                    #html 불러오기
                    url = "https://openapi.naver.com/v1/search/news.json"
                    headers = {'X-Naver-Client-Id':self.client_id, 'X-Naver-Client-Secret':self.client_secret}
                    params = {'query':word, 'display':100, 'start':start, 'sort':'date'}
                    original_html = requests.get(url, headers=headers, params=params)
                except:
                    break
                
                if original_html.status_code !=200:
                    break
                
                start += 100
                
                json_html = original_html.json()
                newslist = json_html['items']
                urls = []
                created_list = []
                for item in newslist:
                    if "news.naver.com" in item['link']:
                        dates = item['pubDate'].split(" ")
                        date = datetime.strptime(dates[3]+"-"+dates[2]+"-"+dates[1]+" "+dates[4], '%Y-%b-%d %H:%M:%S')
                        created_list.append(date)
                        urls.append(item['link'])
                
                final_urls = list(set(urls))
                
                for url, date in zip(final_urls, created_list):
                    try:
                        config = Config()
                        config.request_timeout = 10
                        
                        article = Article(url, language = 'ko', config=config)
                        time.sleep(random.uniform(5,10))
                        article.download()
                        article.parse()

                        NewsFeed = article.text

                        final_info_emb_tuple['name'] = article.title
                        final_info_emb_tuple['description'] = NewsFeed
                        final_info_emb_tuple['link'] = url
                        final_info_emb_tuple['created'] = date
                        final_info_emb_tuple['type'] = "news"
                        final_info_emb_tuple['category'] = "embezzlement"

                        final_info_emb_list.append(final_info_emb_tuple)
                        final_info_emb_list = list(map(dict, set(tuple(sorted(d.items())) for d in final_info_emb_list)))
                        embezzlement_return_list.append(final_info_emb_list)
                    except:
                        pass
            
                try:
                    embezzlement__final_case = np.concatenate(embezzlement_return_list).tolist()
                    final_case = list(map(dict, set(tuple(sorted(d.items())) for d in embezzlement__final_case)))

                    deld = final_case[:]
                    d = []
                    e = []
                    for i in final_case:
                        if not i in d:
                            if not i in e:
                                for j in deld:
                                    if not i == j:
                                        sm = difflib.SequenceMatcher(None, i['name'], j['name'])
                                        similar = sm.ratio()
                                        if similar > 0.25:
                                            d.append(j)
                                        else:
                                            pass
                                    else:
                                        pass
                                e.append(i)

                    final_case = [x for x in final_case if x not in d]
                    
                    db = db_update.update(final_case)
                    """크롤링 결과를 DB에 INSERT함"""
                    db.update_cases_db()
                    db.update_case_tag_db()
                    db.update_event_case_db()
                except:
                    pass