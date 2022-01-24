#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import tabula as tb
import numpy as np
import os
import glob
import gc
from tqdm import tqdm

pdf_data_list = glob.glob("*.pdf")

df_voters_all = pd.DataFrame()

for pdf_data in tqdm(pdf_data_list):

    df_voters = tb.read_pdf(input_path=pdf_data, 
                pages='all', 
                stream=True,
                guess=True,
                area=[150,30,750,576],
                multiple_tables = False,
                pandas_options = {'names' : ["col1","col2","col3","col4","col5"], 'skiprows':1})

    df_voters = df_voters[0]

    drop_row = ['No.', 'N.ame and Signature', '(Member)',
             'We CERTIFY that this list is true and correct and that the same is complete for the precinct.',
             'ELECTION REGISTRATION BOARD / BOARD OF ELECTION INSPECTORS', 'Name. and Signature', '(Chairman)','Date',
             'Name and Sig', '(Member.']

    column_names = df_voters.columns.to_list()

    for drow in drop_row:
        for col_name in column_names:
    
            index_list = df_voters[(df_voters[col_name] == drow)].index.tolist()
            df_voters.drop(index_list, axis=0, inplace=True)
            index_list.clear()

#column_names.clear()
#column_names = df_voters.columns.to_list()
#index_list = []
#index_list_del = []


#for drow in drop_row:
#    for col_name in column_names:
    
#        index_lst = df_voters[(df_voters[col_name] == drow)].index.tolist()
#        index_list.append(index_lst)
        
    df_voters[['No','Category']] = df_voters['col1'].str.split(" ", n = 1, expand = True)
    df_voters.drop('col1', axis = 1, inplace=True)

    df_voters = df_voters[['No','Category','col2',"col3",'col4',"col5"]]

    df_voters.rename(columns={"col2":'Category1', 
                          "col3":'Name', 
                          "col4":'Address_1'}, 
                 inplace=True)

    column_names.clear()
    column_names = df_voters.columns.to_list()

    colname = column_names[-1]
    len_df = len(df_voters)
    len_last_col = df_voters[colname].isnull().sum()

    if len_df == len_last_col:
        df_voters.drop(colname, axis=1, inplace=True)

    column_names.clear()
    column_names = df_voters.columns.to_list()
    col_names = [column_names[-1]]

    for col_name in col_names:
        exec(f'df_na_{col_name} = df_voters[(df_voters[col_name]).isnull()].copy()')
        exec(f'df_notna_{col_name} = df_voters[(df_voters[col_name]).notnull()].copy()')

    df_na_Address_1.drop('Address_1', axis = 1, inplace = True)
    df_na_Address_1['Address'] = df_na_Address_1['Name']
    df_na_Address_1['Name'] = df_na_Address_1['Category1']
    df_na_Address_1.drop('Category1', axis = 1, inplace = True)

    df_notna_Address_1.rename(columns={'Address_1':'Address'}, inplace=True)

    df_voters = pd.concat([df_notna_Address_1, df_na_Address_1])

    for col_name in col_names:
        exec(f'del df_na_{col_name}')
        exec(f'del df_notna_{col_name}')

    df_voters.sort_index(inplace=True)
    df_voters.reset_index(inplace=True)
    df_voters.drop('index', axis = 1, inplace=True)

    df_voters['Category'] = df_voters['Category'].combine_first(df_voters['Category1'])
    df_voters.drop('Category1', axis = 1, inplace = True)

    column_names.clear()
    column_names = df_voters.columns.to_list()
    col_names = [column_names[-1]]

    for col_name in col_names:
        exec(f'df_na_{col_name} = df_voters[(df_voters[col_name]).isnull()].copy()')
        exec(f'df_notna_{col_name} = df_voters[(df_voters[col_name]).notnull()].copy()')
    
    df_na_Address['Address'] = df_na_Address['Name']
    df_na_Address['Name'] = df_na_Address['Category']
    df_na_Address['Category'] = None
    df_voters = pd.concat([df_notna_Address, df_na_Address])

    for col_name in col_names:
        exec(f'del df_na_{col_name}')
        exec(f'del df_notna_{col_name}')

    df_voters.sort_index(inplace=True)
    df_voters.reset_index(inplace=True)
    df_voters.drop('index', axis = 1, inplace=True)

    df_voters[['LastName', 'FirstMiddleName']] = df_voters['Name'].str.split(",", n = 1, expand = True)
    df_voters.drop('Name', axis = 1, inplace = True)
    df_voters[['FirstName', 'MiddleName']] = df_voters['FirstMiddleName'].str.rsplit(" ", n = 1, expand = True)
    df_voters["FirstName"] = df_voters["FirstName"].str.strip()
    df_voters["LastName"] = df_voters["LastName"].str.strip()
    df_voters.drop('FirstMiddleName', axis = 1, inplace = True)


    middlename = [' DE', ' DEL', ' DELA', ' SAN', ' DELOS', ' DE LOS', ' DE LA', ' STO']
    df_fname = pd.DataFrame()
    df_mname = pd.DataFrame()

    for mname in middlename:
        df_fn = df_voters["FirstName"][(df_voters["FirstName"].str.endswith(mname))].str.rsplit(n = 1, expand=True)
        df_fname = df_fname.append(df_fn)

    
    df_fname = df_fname.rename(columns={0:'fname', 1:'mname'})
    df_mn =  pd.DataFrame(df_fname['mname'] + " " + df_voters['MiddleName'])
    df_mname = df_mname.append(df_mn)

    df_voters = pd.concat([df_voters, df_fname, df_mname], ignore_index=True, axis = 1 )

    df_voters[4] = df_voters[6].combine_first(df_voters[4])
    df_voters[5] = df_voters[8].combine_first(df_voters[5])
    df_voters.drop([6,7,8], axis = 1, inplace=True)

    df_voters = df_voters[[0,1,3,4,5,2]]

    df_voters = df_voters.rename(columns={0:'No', 1:'Category', 2:'Address', 3:'LastName', 
                                      4:'FirstName', 5:'MiddleName'})

    df_voters['Category'].fillna('', inplace=True)
    df_voters.drop_duplicates(inplace = True)
    df_voters["MiddleName"] = df_voters["MiddleName"].str.strip()

    del df_fn
    del df_fname
    del df_mn
    del df_mname

######

    df_cat = (df_voters['Category'].str.split("", expand=True))
    
    if len(df_cat.columns) == 3:
        df_cat.rename(columns={1:'cat1'}, inplace=True)
        df_cat.drop([0,2], axis=1, inplace= True)
    elif len(df_cat.columns) == 4:
        df_cat.rename(columns={1:'cat1', 2:'cat2'}, inplace=True)
        df_cat.drop([0,3], axis=1, inplace= True)
    else:
        df_cat.rename(columns={1:'cat1', 2:'cat2', 3:'cat3'}, inplace=True)
        df_cat.drop([0,4], axis=1, inplace= True)

    if len(df_cat.columns) == 1:
        df_voters[['cat1']] = (df_voters['Category'].str.split("", expand=True))
    elif len(df_cat.columns) == 2:
        df_voters[['cat1', 'cat2']] = (df_voters['Category'].str.split("", expand=True).drop([0,3], axis=1))
    else:
        df_voters[['cat1', 'cat2','cat3']] = (df_voters['Category'].str.split("", expand=True).drop([0,4], axis=1))
    

    cat_index = ['A','B','C']
    cat_cols = df_cat.columns.tolist()
    colsindex = []

    for cindex in cat_index:
        for ccols in cat_cols:
            exec(f'df_{ccols}_{cindex} = pd.DataFrame(columns = ["{ccols}""_""{cindex}"])')
            exec(f'df_{ccols}_{cindex} = (df_voters["{ccols}"]=="{cindex}")')
            exec(f'df_{ccols}_{cindex} = (df_voters["{ccols}"]=="{cindex}")')


    if len(df_cat.columns) == 1:
        df_colsindex = pd.concat([df_cat1_A,
                                  df_cat1_B, 
                                  df_cat1_C], 
                                 ignore_index=True, axis = 1)
    elif len(df_cat.columns) == 2:
        df_colsindex = pd.concat([df_cat1_A,df_cat2_A,
                                  df_cat1_B,df_cat2_B, 
                                  df_cat1_C,df_cat2_C], 
                                 ignore_index=True, axis = 1)
    else:
        df_colsindex = pd.concat([df_cat1_A,df_cat2_A,df_cat3_A,
                                  df_cat1_B,df_cat2_B,df_cat3_B, 
                                  df_cat1_C,df_cat2_B,df_cat3_C], 
                                 ignore_index=True, axis = 1)
    
    df_voters.drop(cat_cols, axis =1, inplace = True)
    df_voters.drop('Category', axis =1, inplace = True)

#    if len(df_cat.columns) == 1:
#        df_colsindex['cat1'] = df_colsindex[0].replace(False, np.nan).combine_first((df_colsindex[1].replace(False, np.nan).combine_first(df_colsindex[2])))
#    df_colsindex['cat2'] = df_colsindex[3].replace(False, np.nan).combine_first((df_colsindex[4].replace(False, np.nan).combine_first(df_colsindex[5])))
#    df_colsindex['cat3'] = df_colsindex[6].replace(False, np.nan).combine_first((df_colsindex[7].replace(False, np.nan).combine_first(df_colsindex[8])))
#        df_colsindex.drop([0,1,2], axis =1, inplace = True)
    if len(df_cat.columns) == 2:
        df_colsindex['cat1'] = (df_colsindex[0].replace(False, np.nan).combine_first(df_colsindex[1]).copy())
        df_colsindex['cat2'] = (df_colsindex[2].replace(False, np.nan).combine_first(df_colsindex[3]).copy())
        df_colsindex['cat3'] = (df_colsindex[4].replace(False, np.nan).combine_first(df_colsindex[5]).copy())
        df_colsindex.drop([0,1,2,3,4,5], axis =1, inplace = True)
    else:
        df_colsindex['cat1'] = df_colsindex[0].replace(False, np.nan).combine_first((df_colsindex[1].replace(False, np.nan).combine_first(df_colsindex[2])).copy())
        df_colsindex['cat2'] = df_colsindex[3].replace(False, np.nan).combine_first((df_colsindex[4].replace(False, np.nan).combine_first(df_colsindex[5])).copy())
        df_colsindex['cat3'] = df_colsindex[6].replace(False, np.nan).combine_first((df_colsindex[7].replace(False, np.nan).combine_first(df_colsindex[8])).copy())
        df_colsindex.drop([0,1,2,3,4,5,6,7,8], axis =1, inplace = True)
    
    df_voters = pd.concat([df_voters, df_colsindex], ignore_index=True, axis = 1)

    df_voters.rename(columns={0:'No', 1:'LastName', 2:'FirstName', 3:'MiddleName', 
                              4:'Address', 5:'Illiterate',6:'PWD',7:'SeniorCitizen'}, inplace=True)

    
    classification = ["Illiterate","PWD","SeniorCitizen"]
    for classi in classification:
        df_voters[classi].replace(1, True, inplace = True)
    
    df_brgy = tb.read_pdf(input_path=pdf_data, 
        pages='all', 
        stream=True,
        guess=True,
        area=[70,270,85,380],
        multiple_tables = False)

    df_brgy = df_brgy[0]

    column_names.clear()
    column_names = df_brgy.columns.tolist()
    brgy_colname = column_names[0]
    df_brgy.rename(columns={brgy_colname:'Barangay'}, inplace=True)
    df_brgy.drop_duplicates(inplace=True)

    char_brgy = ['a','r','n','g','y',':',' ']
    for char in char_brgy:
        df_brgy['Barangay'] = df_brgy['Barangay'].str.replace(char,"")

    df_voters["Barangay"] = df_brgy["Barangay"].iloc[[0]].tolist().pop(0)

    df_prec = tb.read_pdf(input_path=pdf_data, 
                pages='all', 
                stream=True,
                guess=True,
                area=[140,45,150,90],
                multiple_tables = False)

    df_prec = df_prec[0]

    column_names.clear()
    column_names = df_prec.columns.tolist()
    prec_colname = column_names[0]

    df_prec.rename(columns={prec_colname:'Precinct'}, inplace=True)
    df_prec.drop_duplicates(inplace=True)
    df_prec.reset_index(inplace=True)
    df_prec.drop('index', axis = 1, inplace=True)

    index_list = df_voters[(df_voters['No']=='1')].index.tolist()
    index_list.pop(0)

    dfs_voters = np.split(df_voters, index_list, axis = 0)

    ind_range = range(len(index_list) + 1)
    df_voters_prec = pd.DataFrame()
    for ind in ind_range:
        exec(f'dfs_voters_{ind} = dfs_voters[ind]')
        exec(f'dfs_voters_{ind}["Precinct"] = df_prec["Precinct"].iloc[[ind]].tolist().pop(0)')
        exec(f'df_voters_prec = df_voters_prec.append(dfs_voters_{ind})')
    
    for ind in ind_range:
        exec(f'del dfs_voters_{ind}')
    

    df_voters = df_voters_prec.copy()
    df_voters_all = df_voters_all.append(df_voters)
    
    pdf_save = pdf_data.replace(".pdf",".csv")
    df_voters.to_csv(pdf_save, encoding = 'utf-8-sig', index = False)
    
    del df_brgy
    del df_colsindex
    del df_prec
    del df_cat
    del df_voters_prec
    del df_voters
    
    gc.collect()

df_voters_all.reset_index(drop=True, inplace = True)

#####

df_cluster_prec = pd.read_csv("clustered precinct.csv")
cluster = df_cluster_prec["Cluster"].tolist()
#cluster_list = []
df_voters_cp = pd.DataFrame()
l = 0
i = 1

for c in cluster:
    exec(f'df_{c} = pd.DataFrame()')
    #x = "cluster_" + str(c)
    #cluster_list.append(x)
    cluster_prec = df_cluster_prec["Precincts"].iloc[l].replace(" ", "").split(",")
    l = l + i
    
    for p in cluster_prec:
        
        cprec = df_voters_all[(df_voters_all["Precinct"] == p)]
        exec(f'df_{c} = df_{c}.append(cprec)')
        exec(f'df_{c}["Cluster"] = df_cluster_prec["Cluster"].iloc[l - 1]')
        exec(f'df_voters_cp = df_voters_cp.append(df_{c}, ignore_index = False)')

#df_voters_cp.drop_duplicates().sort_index()  

for i in cluster:
    exec(f'del df_{i}')
    

df_voters_all = df_voters_cp.sort_index().drop_duplicates()
#df_voters_all = df_voters_all.append(df_voters)

del df_cluster_prec
del df_voters_cp   
del cprec

gc.collect()

### Inserting gender based on first name guessed manually in csv file

df_firstname = pd.read_csv("FirstNameGender.csv")
df_voters_all.insert(loc = 5, column = "Sex",value = "")
for fn in tqdm(df_firstname["FirstName"].tolist()):
    df_voters_all.loc[(df_voters_all["FirstName"] == fn), "Sex"] = df_firstname["Sex"][(df_firstname["FirstName"] == fn)].tolist().pop(0)
    

ipln = pd.read_csv("ip_lastname.csv")
ipad = pd.read_csv("ip_address.csv")
df_voters_all.insert(loc = 6, column = "I.P.",value = False)

for ad in tqdm(ipad["Address"].sort_values().tolist()):
    for ln in ipln["LastName"].sort_values().tolist():
        df_voters_all.loc[(df_voters_all["Address"] == ad) & (df_voters_all["LastName"] == ln),"I.P."] = True       
        

df_voters_all.to_csv("df_voters_all.csv", encoding = 'utf-8-sig', index = False)


# In[ ]:


ipln = pd.read_csv("ip_lastname.csv")
ipad = pd.read_csv("ip_address.csv")
df_voters_all.insert(loc = 6, column = "I.P.",value = False)

for ad in tqdm(ipad["Address"].sort_values().tolist()):
    for ln in ipln["LastName"].sort_values().tolist():
        df_voters_all.loc[(df_voters_all["Address"] == ad) & (df_voters_all["LastName"] == ln),"I.P."] = True
        
        

