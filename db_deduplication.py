import collections
import difflib
import re

class deduplication:
    """Case 중복 제거"""
    def cases_db(cat, cursor,final_case):
        sql = '''SELECT name FROM cases'''
        cursor.execute(sql)
        names = cursor.fetchall()
        
        del_names = []
        for name in range(len(names)):
            for i in range(len(final_case)):
                if final_case[i]['name'] == names[name]['name']:
                    del_names.append(final_case[i])
        
        final_case = [x for x in final_case if x not in del_names]
        
        d = []
        e = []
        for i in final_case:
            if not i in d:
                if not i in e:
                    for j in names:
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
        
        sql = '''SELECT name FROM tags'''
        cursor.execute(sql)
        names = cursor.fetchall()
        
        del_names = []
        for i in range(len(final_case)):
            num = 0
            for name in range(len(names)):
                if re.compile(names[name]['name'], re.I).findall(final_case[i]['description']):
                    num += 1
            
            if num != 0:
                pass
            else:
                del_names.append(final_case[i])
        
        final_case = [x for x in final_case if x not in del_names]
        
        return final_case

    def cases_tags(cat, cursor, data):
        """cases_tags 중복제거"""
        sql = '''SELECT * FROM cases_tags'''
        cursor.execute(sql)
        m_ct_id = cursor.fetchall()

        del_data = []
        for i in range(len(data)):
                for j in range(len(m_ct_id)):
                    m_ct_idL = []
                    m_ct_idL.extend([m_ct_id[j]['case_id'], m_ct_id[j]['tag_id']])
                    if collections.Counter(data[i]) == collections.Counter(m_ct_idL):
                        del_data.append(data[i])

        data = [x for x in data if x not in del_data]

        return data
    
    def events_cases(cat, cursor, data):
        """events_cases 중복제거"""
        sql = '''SELECT * FROM events_cases'''
        cursor.execute(sql)
        m_ec_id = cursor.fetchall()

        del_data = []
        for i in range(len(data)):
                for j in range(len(m_ec_id)):
                    m_ec_idL = []
                    m_ec_idL.extend([m_ec_id[j]['event_id'], m_ec_id[j]['case_id']])
                    if collections.Counter(data[i]) == collections.Counter(m_ec_idL):
                        del_data.append(data[i])

        data = [x for x in data if x not in del_data]

        return data