import re


## Function and it's revers for make short version of DOI
def short_DOI(DOI):
    try:
        x = DOI.split("org/")[1]
        x = x.replace('.', '-')
        x = x.replace('/', '_')
        return(x)
    except:
        print("__hjuston, mamy problem"+DOI)
        return("__hjuston, mamy problem"+DOI)

def long_DOI(DOI):
    x = DOI.replace('-', ',')
    x = DOI.replace('_', '/')
    x = 'https://doi.org/' + x
    return(x)

# Create ID
def create_Article_ID(rowOrDOI, DOI = "doi", title = "title", joural = 'journal', date = 'date', old = False):
    if isinstance(rowOrDOI, str):
        return(short_DOI(rowOrDOI))       

    elif rowOrDOI[DOI] != "empty":
        return(short_DOI(rowOrDOI[DOI]))
    else:
        if old:
            t_old = rowOrDOI[title]+str(rowOrDOI[joural])
            return(hash(t_old))
        
        
        
        else:
            i = 0
            t_new = list('_'*40)
            
            # Tittle
            for t in rowOrDOI[title]:
                if i == 22: break
                t_new[i] = t
                i+=1
            
            i = 23
            
            # Journal
            for t in str(rowOrDOI[joural]):
                if i == 35: break
                t_new[i] = t
                i+=1
                
            
            i = 36
            # Date
            str_date = clean_date(rowOrDOI[date])
            for t in str_date:
                if i == 40: break
                t_new[i] = str(t)
                i+=1
            
            return ''.join(t_new)
  

def open_SONaa(file):
    import json
    json_file = open(file, encoding="utf8")
    List_of_articles = json.load(json_file)
    return(List_of_articles)


# clean_date
def clean_date(date):
    if date == 'unrecovered': return(date)
    elif len(date) == 4: return(date)
    else:
        s = re.split('-|/',date)
    s = [item for item in s if len(item)==4]
    return(s[0])

import pandas as pd
raw_article_list = '../data/Orcid_raw_article_list.csv'
raw_article_list = pd.read_csv(raw_article_list)

new_id = []
for i, row in raw_article_list.iterrows():
    new_id.append(create_Article_ID(row))
    