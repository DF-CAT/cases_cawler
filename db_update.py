import pymysql
import db_deduplication
import re

#cat DB 연결
cat = pymysql.connect(host='df-cat.cauzgq4qtc0a.ap-northeast-2.rds.amazonaws.com', user='admin', password='tqxHrxYQHzwtQ8Y4Odud',db='cat', charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)

"""DB INSERT"""
class update:
    def __init__(self, final_case):
        self.final_case = final_case
    
    """크롤링한 Case들의 결과를 DB에 INSERT"""
    def update_cases_db(self):
        cursor = cat.cursor()
        data = []
        
        """크롤링한 결과에 대한 중복 확인"""
        self.final_case = db_deduplication.deduplication.cases_db(cat, cursor, self.final_case)
        
        """한번에 DB에 넣기 위해 executemany를 사용"""
        try:
            if self.final_case[0]['type'] == "news":
                try:
                    for i in range(len(self.final_case)):
                        values = [str(self.final_case[i]['type']), str(self.final_case[i]['created']), str(self.final_case[i]['name']), str(self.final_case[i]['description']), str(self.final_case[i]['link']), str(self.final_case[i]['category'])]
                        data.append(values)

                    sql = '''INSERT INTO `cases` (type, created, name, description, link, category) VALUES (%s, %s, %s, %s, %s, %s);'''
                    cursor.executemany(sql, data)
                except:
                    pass
            else:
                try:
                    for i in range(len(self.final_case)):
                        values = [str(self.final_case[i]['name']), str(self.final_case[i]['description']), str(self.final_case[i]['case_id']), str(self.final_case[i]['type']),
                                str(self.final_case[i]['created']), int(self.final_case[i]['prec_serial_num']), str(self.final_case[i]['case_name']), str(self.final_case[i]['sentence']),
                                str(self.final_case[i]['court_name']), int(self.final_case[i]['court_code']), str(self.final_case[i]['case_type']), int(self.final_case[i]['case_code']),
                                str(self.final_case[i]['discriminant_type']), str(self.final_case[i]['holding']), str(self.final_case[i]['thrust']), str(self.final_case[i]['refer_prec']),
                                str(self.final_case[i]['category']), str(self.final_case[i]['link']), str(self.final_case[i]['refer_provision'])]
                        data.append(values)

                    sql = '''INSERT INTO `cases` (name, description, case_id, type, created, prec_serial_num, case_name, sentence, court_name, court_code, case_type, 
                                        case_code, discriminant_type, holding, thrust, refer_prec, category, link, refer_provision) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
                    cursor.executemany(sql, data)
                except:
                    pass
        except:
            pass

    def update_case_tag_db(self):
        cursor = cat.cursor()
        """cases_tags DB에 INSERT"""
        sql = '''SELECT id, name, category FROM tags'''
        cursor.execute(sql)
        tag = cursor.fetchall()

        sql = '''SELECT id, description, case_name, category FROM cases'''
        cursor.execute(sql)
        case = cursor.fetchall()

        match = []

        """tag에 있는 단어가 case의 본문에 있으면 매칭"""
        for i in range(len(case)):
            for j in range(len(tag)):
                match_dict = {}
                if case[i]['category'] == tag[j]['category']:
                    if re.compile(tag[j]['name'],re.I).findall(case[i]['description']):
                        match_dict['case_id'] = case[i]['id']
                        match_dict['tag_id'] = tag[j]['id']
                        match.append(match_dict)
        
        """tag에 있는 단어가 case의 reper에 있으면 매칭"""
        for i in range(len(case)):
            for j in range(len(tag)):
                match_dict = {}
                if case[i]['category'] == tag[j]['category']:
                    if case[i]['case_name'] != None:
                        if re.compile(tag[j]['name'],re.I).findall(case[i]['case_name']):
                            match_dict['case_id'] = case[i]['id']
                            match_dict['tag_id'] = tag[j]['id']
                            match.append(match_dict)
                
        data = []

        match = list(map(dict, set(tuple(sorted(d.items())) for d in match)))
        
        for i in range(len(match)):
            values = [int(match[i]['case_id']), int(match[i]['tag_id'])]
            data.append(values)
        
        """중복제거"""
        data = db_deduplication.deduplication.cases_tags(cat, cursor, data)
        
        sql = '''INSERT INTO `cases_tags` (case_id, tag_id) VALUES (%s, %s);'''
        cursor.executemany(sql, data)
    
    def update_event_case_db(self):
        cursor = cat.cursor()
        """events_cases DB에 INSERT"""
        sql = '''SELECT event_id, tag_id FROM events_tags'''
        cursor.execute(sql)
        et_id = cursor.fetchall()

        sql = '''SELECT case_id, tag_id FROM cases_tags'''
        cursor.execute(sql)
        ct_id = cursor.fetchall()

        match = []

        """event와 case가 매칭되어있는 tag가 맞으면 event_id와 case_id를 매칭"""
        for i in range(len(et_id)):
            for j in range(len(ct_id)):
                match_dict = {}
                if et_id[i]['tag_id'] == ct_id[j]['tag_id']:
                    match_dict['event_id'] = et_id[i]['event_id']
                    match_dict['case_id'] = ct_id[j]['case_id']
                    match.append(match_dict)
        
        match = list(map(dict, set(tuple(sorted(d.items())) for d in match)))
        
        data = []

        for i in range(len(match)):
            values = [int(match[i]['event_id']), int(match[i]['case_id'])]
            data.append(values)
        
        """중복제거"""
        data = db_deduplication.deduplication.events_cases(cat, cursor, data)

        sql = '''INSERT INTO `events_cases` (event_id, case_id) VALUES (%s, %s);'''
        cursor.executemany(sql, data)