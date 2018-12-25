
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import Normalize

import numpy as np
import pandas as pd
import sys
#problems found 
#1) variable names not consistent upper/lowercase and use of dots -> solved in the columns by stripping/replacing/lowercase
#2) some waterbodies connected to multiple waterschappen, also changed over the years -> only use latest year
#3) duplicates exist, check for an example NL99_LOM for ammonium -> drop_duplicates
#4) in database oordelen waterbodies exist that are not available in database lokaties ->> created addendum.csv
#5) data vechtstromen ontbreekt in 2016, factsheets zijn wel beschikbaar -> ??? 
#6) doelenset bevat ook doelen uit eerdere jaren ???
#7) in 2015 en 2017 oordelenset afwijkend verbindend streepje in naam prioritaire stoffen - ubiqutair --> vervangen met editor -->>lange termijn is bij import omzetten naar eenduidige codering
#8) in 2017 is naam van overige waterflora gewijzigd naar overige waterflora-kwaliteit (uniformering) -> vervangen met editor in oude naam -->>lange termijn is bij import omzetten naar eenduidige codering idem specifieke stoffen
#9) naamsveranderingen in waterbeheerdernaam en beheerdercode ----->> created waterbeheerders.csv
#10) waterbeheerderscode completed by adding missing lokaties from 2009, waterbeheerder/stroomgebied by 4lettercode assigned
#11) namen van beheerders soms in "" soms met extra spaties voor de ; .... alle bestanden nagelopen
verbose=True

from IPython.display import display_html

def display_side_by_side(*args):
    html_str=''
    for df in args:
        html_str+=df.to_html()
    display_html(html_str.replace('table','table style="display:inline"'),raw=True)


def load_lokatie():
    sys.stdout.flush()
    verbose=True
    #localdir='/home/corbee/Downloads/'
    locfiles=[
      '3a.oppervlaktewaterlichamen_SGBP2_20151028.csv.gz',
      '3b.oppervlaktewaterlichamen_SGBP2_20170606.csv.gz',
      '3b.oppervlaktewaterlichamen_SGBP2_20171106.csv.gz',
      '3b.oppervlaktewaterlichamen_SGBP2_20181128.csv.gz',  #one new NL18_SCHORE
      '3b.oppervlaktewaterlichamen_addendum.csv',
      '3b.oppervlakte_allwaterbeheerders.csv' # file with all waterbodies 
     ]

    lokaties=pd.DataFrame()
    for filename in locfiles:
        locd=pd.read_csv(filename, sep=';', dtype=object, encoding='latin1')
        locd.columns = locd.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')
        lokaties=pd.concat([lokaties, locd])
    
    #lokaties=pd.read_csv('waterbeheerders.csv', sep=';', dtype=object, encoding='latin1')
    lokaties.columns = lokaties.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')
    oldsize=lokaties.shape[0]
    
    lokaties=lokaties[['waterlichaam_identificatie','stroomgebieddistrict_code',
                   'waterbeheerder_omschrijving', 
                   'versie'
                  ]].drop_duplicates()
      
    lokaties1=lokaties.groupby(['waterlichaam_identificatie', 'stroomgebieddistrict_code','waterbeheerder_omschrijving'])['versie'].max().reset_index()

    #assign values in a reverse sorted list so that the newest name is used first 
    lokaties=lokaties.sort_values('versie', ascending=False).drop_duplicates(['waterlichaam_identificatie','stroomgebieddistrict_code'])

    if verbose:
       print('LOKATIES: datarecords: %d without duplicates: %d (waterbodies: %d)' %(oldsize,lokaties.shape[0], lokaties.waterlichaam_identificatie.nunique()) )
    
    return lokaties

#read all dataframes
#dataframe2009 has many differentcoded waterbodies
#data2009 = '4.oordelen_owl_2009_20140507.csv'
def load_oordeel():
    #localdir='/home/corbee/Downloads/'
    datafiles= ['4.oordelen_owl_2009_20140507.csv.gz',
                '4.oordelen_ow_2014_20141117.csv.gz',
                '4.oordelen_owl_2015_20151028.csv.gz',
                '4.oordelen_owl_2016_20170124.csv.gz',
                '4.oordelen_owl_2017_20171120.csv.gz',
                '4.oordelen_owl_2018_20181116.csv.gz'
                ]
    dataframe=pd.DataFrame()
    for filename in datafiles:
        locd=pd.read_csv(filename, sep=';', dtype=object, encoding='latin1')
        dataframe=pd.concat([dataframe, locd])
    #convert to lowercase without dots and spaces    
    dataframe.columns = dataframe.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')
    #size of dataset
    oldsize=dataframe.shape[0]
    #remove duplicates
    
    print('renaming overige waterflora -> overige waterflora-kwaliteit')
    print('renaming overige relevante verontreinigende stoffen -> Specifieke verontreinigende stoffen')
    
    dataframe.loc[dataframe['typering_omschrijving'] =='Overige waterflora', 'typering_omschrijving'] = 'Overige waterflora-kwaliteit'
    dataframe.loc[dataframe['typering_omschrijving'] =='Overige relevante verontreinigende stoffen', 'typering_omschrijving'] = 'Specifieke verontreinigende stoffen'
    
    dataframe=dataframe.drop_duplicates()
    
    print('OORDEEL: datarecords: %d without duplicates: %d (waterbodies: %d)' %(oldsize,dataframe.shape[0],dataframe.waterlichaam_identificatie.nunique()))
    return dataframe

def load_doelen():
    #localdir='/home/corbee/Downloads/'
    datafiles= [ 
                 #'4.doelen_ow_20141127.csv',
                '4.doelen_owl_20151028.csv.gz',
                '4.doelen_owl_20170606.csv.gz',
                '4.doelen_owl_20171106.csv.gz'
                ]
    rapportage=['2015','2016','2017']
    
    dataframe=pd.DataFrame()
   
    for index in range(len(datafiles)):
        locd=pd.read_csv(datafiles[index], sep=';', dtype=object, encoding='latin1')
        locd.columns = locd.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')
        oldsize=locd.shape[0]
        locd=locd[(locd.rapportagejaar==rapportage[index])]
        if verbose:
          if locd.shape[0]<oldsize:
             print('   %s: -database shrunk due to selection on rapportagejaar ' %datafiles[index])            
        
        dataframe=pd.concat([dataframe, locd])
    
    #convert to lowercase without dots and spaces    
    dataframe.columns = dataframe.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')
    #size of dataset
    oldsize=dataframe.shape[0]
    #number of waterbodies
    #print ('unique waterbodies %d' %dataframe.waterlichaam_identificatie.nunique())
    dataframe=dataframe.drop_duplicates()
    if verbose:
      print('DOELEN: datarecords: %d without duplicates: %d (waterbodies: %d)' %(oldsize,dataframe.shape[0],dataframe.waterlichaam_identificatie.nunique()))
    return dataframe

def check_lokaties(dataframe,lokaties):
    lokaties1=dataframe[['waterlichaam_identificatie','rapportagejaar']]
    lokaties1=lokaties1.drop_duplicates()

    df1=lokaties
    df2=lokaties1
    set1=(df1[~(df1['waterlichaam_identificatie'].isin(df2['waterlichaam_identificatie']) )].reset_index(drop=True))
    set2=(df2[~(df2['waterlichaam_identificatie'].isin(df1['waterlichaam_identificatie']) )].reset_index(drop=True))

    if set1.shape[0]>0:
        if verbose:
          print('  CHECK:some waterbodies missing in dataset, present in lokaties')
          display(set1)
    if set2.shape[0]>0:
        print('  CHECK:waterbodies missing in lokaties, present in dataset')
        #display(set2)
#         writer = pd.ExcelWriter('missing.xlsx')
#         set2.to_excel(writer,'Sheet1')
#         writer.save()
        
    if set1.shape[0]+set2.shape[0]==0:
        if verbose:
          print('  CHECK:all waterbodies in lokaties and dataset ')

def merge_data(dataframe,lokaties):
    #add the districtcode/waterbeheerder to the dataset via veld waterlichaam_identificatie
    dataset = pd.merge(dataframe,
    lokaties[['waterlichaam_identificatie','stroomgebieddistrict_code','waterbeheerder_omschrijving' ]],on='waterlichaam_identificatie')
    oldsize=dataset.shape[0]
    dataset=dataset.drop_duplicates()
    if verbose:
      print('  MERGE: datarecords: %d without duplicates: %d (waterbodies: %d)' %(oldsize,dataset.shape[0],dataset.waterlichaam_identificatie.nunique()))
    
    jaren=dataset['rapportagejaar'].unique()
    print(jaren)
    
    return dataset

def show_waterbodies_waterbeheerder(dataframe):
     sub=dataframe.groupby(['stroomgebieddistrict_code','waterbeheerder_omschrijving','rapportagejaar']) ['waterlichaam_identificatie'].nunique().to_frame('count').reset_index()
     display(pd.pivot_table(sub, index=['stroomgebieddistrict_code','waterbeheerder_omschrijving'], columns=['rapportagejaar'], values=['count'],
            aggfunc={'count':[np.sum]},fill_value=0))

def show_waterbodies_stroomgebied(dataframe):
     sub=dataframe.groupby(['stroomgebieddistrict_code','waterbeheerder_omschrijving','rapportagejaar']) ['waterlichaam_identificatie'].nunique().to_frame('count').reset_index()
     
     display(pd.pivot_table(sub, index=['stroomgebieddistrict_code'], columns=['rapportagejaar'], values=['count'],
            aggfunc={'count':[np.sum]},fill_value=0))
  
def draw_oordeelplot(sub1, item,show_table, selector,oordeeltype ):
            fig, (ax1, ax2) = plt.subplots(figsize=[15,3], ncols=2, sharey=False)
            
            #generate labels based on selected data
            oordelen=sub1['oordeel_overall'].unique()
            
            #check two versions
            if oordeeltype=='multi':
                omap= { 'oordeel_overall':{ 0:'slecht',1:'ontoereikend',2:'matig',3:'goed',4:'zeer goed'} }
            else:
                omap= { 'oordeel_overall':{ 0:'voldoet niet',1:'voldoet'}}
            
            sub1=sub1.replace(omap)
            
            #sort data to present graphs properly           
            sub1=sub1.sort_values(['rapportagejaar'])
            rapportagejaren=sub1['rapportagejaar'].unique()
            
            #generate plots  
            a=pd.crosstab([sub1.rapportagejaar],[sub1.oordeel_overall], normalize=False)
                
            b=pd.crosstab([sub1.rapportagejaar],[sub1.oordeel_overall], normalize='index')  
            b=b.round({'voldoet':3,'voldoet niet':3,'goed':3,'matig':3,'ontoereikend':3,'slecht':3})
            
            if show_table:
              display_side_by_side(a,b)
                        
            #map colours to values
            columnnames = ['slecht','ontoereikend', 'matig', 'goed', 'zeer goed','voldoet', 'voldoet niet']
            color_list = ['red','orange', 'yellow', 'lightgreen', 'blue','lightgreen', 'red']
            colordict = dict(zip(columnnames, color_list))
                        
            a.plot.barh(stacked=True, width=0.8 ,
                        color=map(colordict.get,a.columns),  
                        ax=ax1, title=item +'(waterlichamen)')
            
            b.plot.barh(stacked=True, width=0.8 ,
                        color=map(colordict.get,b.columns),  
                        ax=ax2, title=item+ '(percentage)', xlim=[0,1] )

            ax1.set_yticklabels(rapportagejaren)
            ax1.set_ylabel('')
            ax1.set_xlabel(selector)
            ax1.legend().set_visible(False)

            ax2.set_yticklabels(rapportagejaren)
            ax2.set_ylabel('')
            ax2.set_xlabel(selector)
            ax2.legend().set_visible(False)

            handles, labels = ax1.get_legend_handles_labels()
                                    
            fig.legend(handles, labels, loc='lower center')
            #ax1 = plt.gca()
            ax1.invert_yaxis()
            #ax2 = plt.gca()
            ax2.invert_yaxis()

            plt.show()
            
def oordeeluniform(subs):
    #first change categorical data into numerical ordered data
       
    map= { 'oordeel':{ 'slecht':0,'ontoereikend':1,'matig':2,'goed':3,'zeer goed':4,'voldoet niet':0,'voldoet':1} }
    sub3=subs.replace(map)
      
    #create a crosstable based on the oordeel (only allow aquo and desk)
    sub3a=sub3[(sub3.waardebepalingsmethode_code=='AQUOKIT')]
    sub3b=sub3[(sub3.waardebepalingsmethode_code=='DESK')]
    #join both datasets
    sub3=pd.concat([sub3a,sub3b])
    
    sub4=pd.pivot_table(sub3, index=['stroomgebieddistrict_code','waterbeheerder_omschrijving','waterlichaam_identificatie','rapportagejaar'], columns=['waardebepalingsmethode_code'], values=['oordeel']).reset_index()
    #create combined_names for the multiindexx columns
    sub4.columns = sub4.columns.map('_'.join)
    #strip the non-multiindex from the _ 
    sub4=sub4.rename(index=str, columns={'stroomgebieddistrict_code_': 'stroomgebieddistrict_code',
                                    'waterbeheerder_omschrijving_': 'waterbeheerder_omschrijving',
                                    'waterlichaam_identificatie_': 'waterlichaam_identificatie',
                                    'rapportagejaar_': 'rapportagejaar'
                                   })
    desk=('oordeel_DESK' in sub4)
    aquo=('oordeel_AQUOKIT' in sub4)
    
    
    if desk&aquo:
          sub4['oordeel_overall'] = np.where(sub4['oordeel_DESK'].isnull(), sub4['oordeel_AQUOKIT'], sub4['oordeel_DESK'])
    else: 
        if aquo: 
          sub4['oordeel_overall'] =sub4['oordeel_AQUOKIT']
        if desk:
          sub4['oordeel_overall'] =sub4['oordeel_DESK']
    return sub4
   
                
     
def display_stroomgebied_oordeel(subs, show_table, selector):
    
    #subs=subs[(subs.waterbeheerder_omschrijving=="Waterschap Hunze en Aa's")]
    #subs=subs[(subs.rapportagejaar>"2009")]
    
    stroomgebieden=subs['stroomgebieddistrict_code'].unique()
    print(stroomgebieden)
    stroomgebieden.sort()
    
    oordelen=subs['oordeel'].unique()
    if 'voldoet' in oordelen:
        oordeeltype='binair'
    else:
       if 'voldoet niet' in oordelen: 
           oordeeltype='binair'
       else:
           oordeeltype='multi'
    
    #create single oordeel per waterbody ! ->need recoding 
    sub4=oordeeluniform(subs)
    
    #create table with values
    sub3=pd.pivot_table(sub4, index=['stroomgebieddistrict_code','waterlichaam_identificatie'], columns=['rapportagejaar'], values=['oordeel_overall']).reset_index()
           
    #calculate differences between two years
    errorfound=False;
    #check if years exist
    try:
      sub3['oordeel_diff']=sub3['oordeel_overall','2018']-sub3['oordeel_overall','2015']
    except:
      errorfound=True  
    if errorfound!=True:    
        #remove rows with possible NaNs in diff
        sub3 = sub3[pd.notnull(sub3['oordeel_diff'])]
        #plot the figure
        fig,ax  = plt.subplots( figsize=[15,4])
        plt.title('Verschillen 2018 - 2015 (aantal waterlichamen)')
        diffs=sub3['oordeel_diff'].unique()
        binsT=diffs.shape[0]
        if binsT<3:
            binsT=3
        ax=plt.hist(sub3['oordeel_diff'],bins=binsT, rwidth=0.9)
        dx=(ax[1][1]-ax[1][0])/2
        for i in range(binsT):
               if ax[0][i]>0:
                   plt.text(ax[1][i]+dx,ax[0][i],str(int(ax[0][i])),fontsize=18, color='black')
        plt.show()
    
    if stroomgebieden.shape[0]==4:
      draw_oordeelplot(sub4,'Nederland',show_table,selector,oordeeltype)
    
    for item in stroomgebieden:
            sub3=sub4[(sub4.stroomgebieddistrict_code==item)]
            if sub3.shape[0]>0:
                draw_oordeelplot(sub3,item, show_table, selector,oordeeltype)

def display_waterbeheerder_oordeel(subset, show_table, selector):
    waterbeheerders=subset['waterbeheerder_omschrijving'].unique()
    waterbeheerders.sort()
    
    oordelen=subset['oordeel'].unique()
    if 'voldoet' in oordelen:
        oordeeltype='binair'
    else:
       if 'voldoet niet' in oordelen: 
           oordeeltype='binair'
       else:
           oordeeltype='multi'
    
    sub4=oordeeluniform(subset)
    for item in waterbeheerders:
            sub1=sub4[(sub4.waterbeheerder_omschrijving==item)]
            if sub1.shape[0]>0:
                 draw_oordeelplot(sub1,item,show_table, selector, oordeeltype)

def display_waterlichaam_oordeel(subset, show_table):
    waterlichamen=subset['waterlichaam_identificatie'].unique()
    waterlichamen.sort()
    for item in waterlichamen:
            sub1=subset[(subset.waterlichaam_identificatie==item)]
            if sub1.shape[0]>0:
                 draw_oordeelplot(sub1,item,show_table)
                    
                    
def display_stof_oordeel(subset, show_table):
    stoffen=subset['chemischestof_omschrijving'].dropna().unique()
    stoffen=sorted(stoffen)
    if len(stoffen)!=0:
        for item in stoffen:
                sub1=subset[(subset.chemischestof_omschrijving==item)]
                if sub1.shape[0]>0:
                     draw_oordeelplot(sub1,item, show_table)
    else:
        stoffen=subset['typering_omschrijving'].dropna().unique()
        stoffen=sorted(stoffen)
        for item in stoffen:
                sub1=subset[(subset.typering_omschrijving==item)]
                if sub1.shape[0]>0:
                     draw_oordeelplot(sub1,item, show_table)
    

def display_stroomgebied_doelen(subset, show_table):
    stroomgebieden=subset['stroomgebieddistrict_code'].unique()
    stroomgebieden.sort()

    for item in stroomgebieden:
            sub1=subset[(subset.stroomgebieddistrict_code==item)]
            sub1=sub1[['rapportagejaar','waterlichaam_identificatie','stroomgebieddistrict_code',  'chemischestof_omschrijving','typering_code','ondergrens_waarde']].drop_duplicates()
           
            display(pd.pivot_table(sub1, index=['stroomgebieddistrict_code','typering_code'], columns=['rapportagejaar'], values=['ondergrens_waarde'], aggfunc={'ondergrens_waarde':[np.max, np.min, np.count_nonzero]},fill_value=0))
            
            typecode=sub1['typering_code'].unique()
            
            for item in typecode:
                    histdata=sub1[(sub1.rapportagejaar=='2017')]
                    histdata=histdata[(histdata.typering_code==item)]
                    histdata=histdata['ondergrens_waarde'].astype(float)
                    
                    if len(histdata)>0:
                    # the histogram of the data
                        num_bins = 10
                        fig, ax = plt.subplots()
                        # the histogram of the data
                        ax.set_xlabel(item)
                        ax.hist(histdata)
                        plt.show()
