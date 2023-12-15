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
def create_Article_ID(row, DOI = "link", title = "title", joural = 'journal'):
    if isinstance(row, str):
        return(short_DOI(row))       

    elif row[DOI] != "empty":
        return(short_DOI(row[DOI]))
    else:
        t = row[title]+str(row[joural])
        return(hash(t))
  

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