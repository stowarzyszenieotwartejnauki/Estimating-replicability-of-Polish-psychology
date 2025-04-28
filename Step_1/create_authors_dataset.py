import pandas as pd
import os
import json
import tools_SONaa
from glob import glob
from tqdm import tqdm

source = '../Step_1/scientists.csv'
save = False
dest = '../main_dataset'
    
df = pd.read_csv(source)

# Selecting columns & changing names to English
df = df[['Id', 'Dane podstawowe - Imię', 'Dane podstawowe - Drugie imię', 'Dane podstawowe - Przedrostek nazwiska', 'Dane podstawowe - Nazwisko', 
            'Zatrudnienie - Nazwa','Zatrudnienie - Podstawowe miejsce pracy', "Zatrudnienie - Oświadczone dyscypliny", "Stopnie naukowe - Stopień naukowy", "Stopnie naukowe - Rok uzyskania stopnia"]]

df.columns = ['id', 'name', 'second_name', 'pre_surname', 'surname', 
            'uni_name', 'is_a_main_job', "declared_discipline", "degree", "degree_year"]

# change univeristy names for interoperability
df=df.replace({'uni_name':'im. '},{'uni_name':''},regex=True)
df=df.replace({'uni_name':'sp. z o.o. '},{'uni_name':''},regex=True)
df=df.replace({'uni_name':' '},{'uni_name':'_'},regex=True)

authors_raw = df

authors = pd.DataFrame()

# Iterating by authors' ids
for i in authors_raw.id.unique():
    
    df = authors_raw.loc[authors_raw["id"] == i]
    
    author = []
    
    for index, row in df.iterrows():

        if row['surname'] == row['surname']:
            author = row[['id', 'name', 'second_name', 'pre_surname', 'surname']]     
        
        if row['is_a_main_job'] == 'Tak':
            author['main_job'] = row['uni_name']
    

    names = []
    fullname = []
    if str(author['name']) != 'nan':
        first_name = author['name']
        fullname.append(first_name)
    if str(author['second_name']) != 'nan':
        second_name = author['second_name']
        fullname.append(second_name)
    if str(author['pre_surname']) != 'nan':
        pre_surname = author['pre_surname']
        fullname.append(pre_surname)
    if str(author['surname']) != 'nan':
        surname = author['surname']
        fullname.append(surname)

    names.append(' '.join(fullname))
    author['fullname'] = names[0]

    authors = pd.concat([authors,author.to_frame().T])
    
    fn = authors['fullname'].to_list()
    Aid = [tools_SONaa.create_Auhor_ID(fullname) for fullname in fn]
    authors['Aid'] = Aid



if save: authors.to_csv(dest+"/all_authors.csv", index=False)


# Upload information about evaluation
evaluation = pd.read_csv('institutions/evaluation_data.csv', index_col="uni_code")

# Selecting scientists who are connected to evaluated institutions
authors_selected = authors[authors['main_job'].isin(evaluation.index)]
if save: authors_selected.to_csv(dest+"/selected_authors.csv", index=False)








import os
import pandas as pd
dest = '../main_dataset'
    
selected_authors = pd.read_csv(dest+"/selected_authors.csv")

# Specify the main folder path
main_folder = "../private_data/institutions"

# Walk through the folder and its subdirectories
for root, dirs, files in os.walk(main_folder):
    for file in files:

        if file == "names.xlsx":
            # Construct the full file path
            file_path = os.path.join(root, file)
            # Load the Excel file into a DataFrame
            df = pd.read_excel(file_path)

            # Iterate over rows in the DataFrame
            for i, row in df.iterrows():
                # Skip rows where "Checked" is not 0
                if row.get("Checked", 1) == 0:  # Use `.get()` for safety in case "Checked" column is missing
                    continue

                # Process ORCID only if it is a valid string
                orcid = None
                if isinstance(row.get("orcid"), str) and ".org/" in row["orcid"]:
                    orcid = row["orcid"].split(".org/")[1]
                else:
                    continue

                # Skip a specific author
                if row.get("fullname") == 'Paulina Michalska':
                    continue

                # Print and update only if the author is in `selected_authors`
                if row.get("fullname") in selected_authors['fullname'].values:
                    selected_authors.loc[selected_authors['fullname'] == row["fullname"], 'orcid'] = orcid
                    
if save: selected_authors.to_csv(dest+"/selected_authors.csv", index=False)



import os
import pandas as pd
dest = '../main_dataset'
    
selected_authors = pd.read_csv(dest+"/selected_authors.csv")

selected_authors.loc[selected_authors['fullname'] == row["fullname"], 'orcid'] = orcid


for i, row in raw_article_list.iterrows():
    if row['name'] == 'Paulina Michalska':
        if row['orcid'] == 'https://orcid.org/0000-0003-2703-158X':
            author_id.append('37752ECB374FD46477A832E60F9B912D7ED8ADC3')
        else:
            author_id.append('5372154B073A4D0484B53F3041ECB2F661B2C36B')
    else:
        id = loa[loa['fullname']==row['name']].reset_index()
        author_id.append(id.at[0,'id'])
