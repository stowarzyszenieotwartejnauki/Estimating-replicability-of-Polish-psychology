# repair duplicated author and add author ID to file


raw_article_list = '../data/raw_article_list.csv'
loa = '../main_dataset/list_of_authors.csv'
raw_article_list = pd.read_csv(raw_article_list)
loa = pd.read_csv(loa)

authors = loa['fullname']
authors[authors.duplicated()]

author_id=[]

for i, row in raw_article_list.iterrows():
    if row['name'] == 'Paulina Michalska':
        if row['orcid'] == 'https://orcid.org/0000-0003-2703-158X':
            author_id.append('37752ECB374FD46477A832E60F9B912D7ED8ADC3')
        else:
            author_id.append('5372154B073A4D0484B53F3041ECB2F661B2C36B')
    else:
        id = loa[loa['fullname']==row['name']].reset_index()
        author_id.append(id.at[0,'id'])

raw_article_list['author_id'] = author_id

raw_article_list.to_csv('../data/raw_article_list.csv')