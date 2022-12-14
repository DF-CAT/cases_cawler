"""
영업 비밀 유출 및 횡령에 대한 크롤러
크롤링은 네이버 뉴스 및 케이스 노트 사이트에서 진행하며
24시간 간격으로 크롤링됨
키워드 기반으로 검색
분류 기준은 Tag가 본문에 들어가 있으면
해당되는 Artifacts로 매칭
"""
import judicial_crawler
import news_crawler
import time
import multiprocessing
import warnings
warnings.filterwarnings('ignore')

def main():
    #영업비밀유출 검색어
    article_information_search = ["영업비밀유출", "영업비밀유출+이동식저장장치", "영업비밀유출+외부 저장장치", "영업비밀유출+USB",
                        "영업비밀유출+외장하드", "영업비밀유출+이메일", "영업비밀유출+출력", "영업비밀유출+클라우드", "영업비밀유출+촬영"]
    precedent_information_search = ["영업비밀유출", "영업비밀유출+이동식저장장치", "영업비밀유출+외부 저장장치", "영업비밀유출+USB",
                        "영업비밀유출+외장하드", "영업비밀유출+이메일", "영업비밀유출+출력", "영업비밀유출+클라우드", "영업비밀유출+촬영"]
    
    #횡령 검색어
    article_embezzlement_search = ["횡령", "배임", "업무상의 횡령", "업무상의 배임", "배임수증재"]
    precedent_embezzlement_search = ["횡령", "배임", "업무상의 횡령", "업무상의 배임"]
    
    #네이버 API
    client_id = "pfAMh81kf05sR2T8h2EB"
    client_secret = "ukfjIUTkpX"
    
    """멀티 프로세스를 이용하여 크롤링함"""
    news = news_crawler.article_crawler(article_information_search, article_embezzlement_search, client_id, client_secret)
    precedent = judicial_crawler.precedent_crawler(precedent_information_search, precedent_embezzlement_search)
    
    crawler_news1 = multiprocessing.Process(target=news.information_crawler, args=())
    crawler_news2 = multiprocessing.Process(target=news.embezzlement_crawler, args=())
    crawler_precedent1 = multiprocessing.Process(target=precedent.information_crawler, args=())
    crawler_precedent2 = multiprocessing.Process(target=precedent.embezzlement_crawler, args=())
    
    crawler_news1.start()
    crawler_news2.start()
    crawler_precedent1.start()
    crawler_precedent2.start()
    
    crawler_news1.join()
    crawler_news2.join()
    crawler_precedent1.join()
    crawler_precedent2.join()

if __name__ == '__main__':
    print("[*] Start [*]")
    start = time.perf_counter()
    
    main()
    
    finish = time.perf_counter()
    """시간이 얼마나 걸리는지 체크"""
    print(f'Finished in {round(finish-start, 2)} second(s)')