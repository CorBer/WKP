import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import Normalize

import numpy as np
import pandas as pd
import sys
#problems found 
#1) variable names not consistent upper/lowercase and use of dots -> solved in the columns by stripping/replacing/lowercase
#2) some waterbodies connected to multiple waterschappen, also changed over the years -> only use latest year as a base
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
Usedeelstroom=False

from IPython.display import display_html
###############################################################################################################################
def display_side_by_side(*args):
    html_str=''
    for df in args:
        html_str+=df.to_html()
    display_html(html_str.replace('table','table style="display:inline"'),raw=True)
###############################################################################################################################    
#general load of all data
def load_merge():    
    #load_maatregel()
    print('lokaties/oordelen ophalen')
    lokaties=load_lokatie()    
    oordeel=load_oordeel()
    check_lokaties(oordeel,lokaties)
    oordeelset=merge_data(oordeel,lokaties)
    
    oordeelset.waterlichaam_type=oordeelset.waterlichaam_type.astype('category')
    print('LOKATIES/OORDELEN loaded/merged. %d records for %d locations' %(oordeelset.shape[0],lokaties.waterlichaam_identificatie.nunique()))
    
    #display(oordeelset.info(memory_usage='deep'))
    #display(oordeelset.memory_usage(deep=True))
    return oordeelset    
###############################################################################################################################    
#simple routine to load lokatie files    
def load_lokatie():
    sys.stdout.flush()
    localdir='datasets/'
    locfiles=[
          '3a.oppervlaktewaterlichamen_SGBP2_20151028.csv.gz',          '3b.oppervlaktewaterlichamen_SGBP2_20170606.csv.gz',
          '3b.oppervlaktewaterlichamen_SGBP2_20171106.csv.gz',          '3b.oppervlaktewaterlichamen_SGBP2_20181128.csv.gz',  
          '3b.oppervlaktewaterlichamen_addendum.csv',          '3b.oppervlakte_allwaterbeheerders.csv' # old wl no waterlichaamtype
         ]
    #load all CSVs
    lokaties=pd.DataFrame()
    #load datafiles and concat them 
    for filename in locfiles:
        locd=pd.read_csv(localdir+filename, sep=';', dtype=object, encoding='latin1')
        locd.columns = locd.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')
        lokaties=pd.concat([lokaties, locd])
    #nr of records originally
    oldsize=lokaties.shape[0]    
    #restrict the fields 
    lokaties=lokaties[['waterlichaam_identificatie','stroomgebieddistrict_code','deelstroomgebieddistrict_code',
                       'waterbeheerder_omschrijving','waterlichaam_type', 'versie'
                      ]].drop_duplicates()
    #if deelstroomgebied is missing at least try to fill with stroomgebied 
    
    lokaties['deelstroomgebieddistrict_code'].fillna(lokaties['stroomgebieddistrict_code'], inplace=True)
    
    #fill any possible NaN with an empty space 
    lokaties.fillna('', inplace=True)
    
    #extract for each waterbody the last available version as default for the waterbeheerdernaam
    # first sort the database then use the last available and then reset_index to prevent a multiindex dataframe
    lokaties.sort_values(['waterlichaam_identificatie','versie'],inplace=True)
    lokaties=lokaties.groupby('waterlichaam_identificatie').last().reset_index()
    if verbose:
       (print('LOKATIES: datarecords: %d without duplicates: %d (waterbodies: %d)' 
              %(oldsize,lokaties.shape[0], lokaties.waterlichaam_identificatie.nunique()) )
       )
    return lokaties

###############################################################################################################################
def load_oordeel():
    localdir='datasets/'
    datafiles= ['4.oordelen_owl_2009_20140507.csv.gz', '4.oordelen_ow_2014_20141117.csv.gz', '4.oordelen_owl_2015_20151028.csv.gz',
                '4.oordelen_owl_2016_20170124.csv.gz', '4.oordelen_owl_2017_20171120.csv.gz', '4.oordelen_owl_2018_20181116.csv.gz'
                ]
    dataframe=pd.DataFrame()
    for filename in datafiles:
        locd=pd.read_csv(localdir+filename, sep=';', dtype=object, encoding='latin1')
        dataframe=pd.concat([dataframe, locd])
    #convert to lowercase without dots and spaces    
    dataframe=dataframe[[#'ChemischeStof.Code', 
                         'ChemischeStof.Omschrijving', 'Grootheid.Code',
       #'Grootheid.Omschrijving', 
        'Hoedanigheid.Code',
       #'Hoedanigheid.Omschrijving', 
       #'Namespace', 
       'Oordeel',
       'Oordeelsoort.Code', 
       #'Oordeelsoort.Omschrijving', 
        'Rapportagejaar',
       #'Typering.Code', 
        'Typering.Omschrijving', 
       #'Versie',
       'WaardebepalingsMethode.Code', 
       #'WaardebepalingsMethode.Omschrijving',
       'Waterlichaam.Identificatie', 
                        # 'last_change_time'
                        ]]
    
    dataframe.columns = dataframe.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')
    #size of dataset
    oldsize=dataframe.shape[0]
    #TODO use dicts on definitions from typering_code/typering_omschrijving based on latest
    if verbose:
        print('renaming overige waterflora -> overige waterflora-kwaliteit')
        print('renaming overige relevante verontreinigende stoffen -> Specifieke verontreinigende stoffen')
    #select all rows for typering_omschrijving with a condition and change the value    
    dataframe.loc[dataframe['typering_omschrijving'] =='Overige waterflora', 'typering_omschrijving'] = 'Overige waterflora-kwaliteit'
    dataframe.loc[dataframe['typering_omschrijving'] =='Overige relevante verontreinigende stoffen', 'typering_omschrijving'] = 'Specifieke verontreinigende stoffen'
    
    #always remove duplicates
    dataframe=dataframe.drop_duplicates()
    if verbose:
        (print('OORDEEL: datarecords: %d without duplicates: %d (waterbodies: %d)' 
               %(oldsize,dataframe.shape[0],dataframe.waterlichaam_identificatie.nunique()))
        )
    return dataframe

###############################################################################################################################
def load_maatregel():
    localdir='datasets/'
    datafiles= ['SGBP_maatregelen2015.csv', 'SGBP_maatregelen2016.csv',   'SGBP_maatregelen2017.csv'  ]
    rapportage=['2015','2016','2017']
    
    maatregel=pd.DataFrame()
   
    for index in range(len(datafiles)):
        
        locd=pd.read_csv(localdir+datafiles[index], sep=';', decimal='.', dtype=object, encoding='latin1')
        print(datafiles[index])
        
        locd=locd[['Waterbeheerder_Naam','Naam','SGBPPeriode.Omschrijving','UitvoeringIn', 'LokaalID',
                        'KRWmaatregel.Code','KRWmaatregel.Naam','Hoofdcategorie','SGBP_Klasse',
                        'SGBP_code','SGBP_artikel','SGBP_Hoofdcategorie','KRWMaatregelNiveau.Code',
                        'KRWMaatregelNiveau.Omschrijving','HoortbijKRWwaterlichaam','KRWstroomgebiedDistrict.Code',
                        'OpnameInSGBPJN','Initiatiefnemer.Code','Initiatiefnemer.Naam','EUFonds.Code',
                        'versie','OmvangNumeriekeWaarde','omvangeenheidcode','voortgang',
                        'Jaar_voortgang','Type_voortgang','Naam_Eenheid']]
        
        
        locd.columns = locd.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')
        #strip if more than oneyear is in the dataset
        #locd['volgjaar']=rapportage[index]
        locd=locd[(locd.jaar_voortgang==rapportage[index])]
        maatregel=pd.concat([maatregel, locd])
    
    #convert to lowercase without dots and spaces    
    maatregel.columns = maatregel.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')
    maatregel.voortgang=maatregel.voortgang.astype(float).round(1)
    
    #g=maatregel.groupby(['krwstroomgebieddistrict_code','jaar_voortgang','naam_eenheid'])
    #display(g.size())
    maatregel=maatregel[(maatregel.uitvoeringin=='2015-2021')]
    maatregel=maatregel[(maatregel.jaar_voortgang=='2017')]
    #maatregel=maatregel[(maatregel.krwstroomgebieddistrict_code=='NLRN')]
    #maatregel=maatregel[(maatregel.waterbeheerder_naam=='Hoogheemraadschap Amstel, Gooi en Vecht')]
    #maatregel=maatregel[(maatregel.naam_eenheid=='vispasseerbaar maken kunstwerk [n]')]
    
    
 #   writer = pd.ExcelWriter('output.xlsx')
 #   pv.to_excel(writer,'Sheet1')
 #   writer.save()
    
    #ordered categories
    mcats=['aanpassen / verplaatsen grondwaterwinning [n]',          'opheffen ongezuiverde lozingen [n]',              'aanpakken riooloverstorten [n]',
          'overige emissiereducerende maatregelen [n]',           'verminderen belasting RWZI [n]',              'afkoppelen verhard oppervlak [ha]',    
          'herstellen lekke riolen [n]',          'saneren verontreinigd(e) landbodems / grondwater [n]',   'verminderen emissie nutriÃ«nten landbouw [n]',
          'overige bronmaatregelen [n]',              
          'saneren uitlogende oeverbescherming [n]',          'verwijderen verontreinigde bagger [m3]',          
          'verminderen emissie verkeer / scheepvaart [n]',          'verminderen emissie gewasbescherming- / bestrijdingsmiddelen [n]',
          'inrichten mest- / spuitvrije zone [n]',          'verminderen emissies bouwmaterialen [n]', 
          'verbreden watersysteem, aansluitend wetland / verlagen uiterwaard [ha]',              'verondiepen watersysteem [ha]',    
          'aanpassen inlaat / doorspoelen / scheiden water [n]',          'aanpassen waterpeil [n]',              'verwijderen stuw [n]',
          'vispasseerbaar maken kunstwerk [n]',              "verbreden / nvo; langzaam stromend / stilstaand water [km]",
          'verdiepen watersysteem  (overdimensioneren) [ha]',          'overige inrichtingsmaatregelen [n]',
          'aanleg nevengeul / herstel verbinding [km]',             'vasthouden water in haarvaten van het systeem [ha]',                                                        
          "verbreden / hermeanderen / nvo; (snel) stromend water [km]",              'gewenste grond- en oppervlaktewaterregime [ha]',    
          'waterbeheer 21e eeuw [n]',          'aanleg speciale leefgebieden voor vis [ha]',                  'aanleg zuiveringsmoeras [ha]',
          'uitvoeren actief vegetatie- / waterkwaliteitsbeheer [km]',              'uitvoeren actief visstands- of schelpdierstandsbeheer [ha]',
          'overige beheermaatregelen [n]',          'wijzigen / beperken gebruiksfunctie [ha]',          'financiele maatregelen [n]',                
          'geven van voorlichting [n]',              'aanleg speciale leefgebieden flora en fauna [n]',          'opstellen nieuw plan [n]',    
          'overige instrumentele maatregelen [n]',              'beheren van grootschalige grondwaterverontreinigingen [n]',    
          'aanpassen begroeiing langs water [km]',          'aanpassen/introduceren (nieuwe) wetgeving [n]',          'overige RO-maatregelen [n]',    
          'uitvoeren onderzoek [n]' ]
          
    #recategorize and order rows 
    maatregel['naam_eenheid'] = maatregel['naam_eenheid'].astype('category', categories=mcats )
    #create pivot
    pv=maatregel.pivot_table(index=['naam_eenheid','jaar_voortgang'], columns='type_voortgang', values='voortgang', aggfunc=[np.sum])
    #reorder the columns 
    cols = ['PLAN', 'UITVOERING', 'UITGEVOERD',  'GEFASEERD','VERVANGEN', 'INGETROKKEN']
    pv = pv.reindex_axis(cols, axis=1, level='type_voortgang')
    
#     display(pv)
    
#     fig, (ax1) = plt.subplots(figsize=[15,30], sharey=False)    
#     ax1.set_xscale("log")       
#     pv.plot.barh(stacked=True, width=0.8 ,
#                         #color=map(colordict.get,a.columns),  
#                         ax=ax1, title='test' )
#     ax1.invert_yaxis()
#     plt.show()
            
    
###############################################################################################################################    
def load_doelen():
    localdir='datasets/'
    datafiles= [ #'4.doelen_ow_20141127.csv',
                '4.doelen_owl_20151028.csv.gz',                '4.doelen_owl_20170606.csv.gz',
                '4.doelen_owl_20171106.csv.gz'
                ]
    rapportage=['2015','2016','2017']
    
    dataframe=pd.DataFrame()
   
    for index in range(len(datafiles)):
        locd=pd.read_csv(localdir+datafiles[index], sep=';', dtype=object, encoding='latin1')
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
    #drop duplicates
    dataframe=dataframe.drop_duplicates()
    if verbose:
      print('DOELEN: datarecords: %d without duplicates: %d (waterbodies: %d)' %(oldsize,dataframe.shape[0],dataframe.waterlichaam_identificatie.nunique()))
    return dataframe
###############################################################################################################################
#check if lokaties and dataset are identical, report differences
def check_lokaties(dataframe,lokaties):
    lokaties1=dataframe[['waterlichaam_identificatie','rapportagejaar']]
    lokaties1=lokaties1.drop_duplicates()
    set1=(lokaties[~(lokaties['waterlichaam_identificatie'].isin(lokaties1['waterlichaam_identificatie']) )].reset_index(drop=True))
    set2=(lokaties1[~(lokaties1['waterlichaam_identificatie'].isin(lokaties['waterlichaam_identificatie']) )].reset_index(drop=True))
    if verbose:  #only report if wanted
        if set1.shape[0]>0: 
          print('  CHECK:some waterbodies missing in dataset, present in lokaties')
          display(set1)
        if set1.shape[0]+set2.shape[0]==0:
          print('  CHECK:all waterbodies in lokaties and dataset ')
    #always report if waterbodies are missing in lokaties !
    if set2.shape[0]>0: 
        print('  CHECK: some waterbodies missing in lokaties, present in dataset')
    
###############################################################################################################################    
#combine dataset with information from lokaties
def merge_data(dataframe,lokaties):
    #add the districtcode/waterbeheerder to the dataset via veld waterlichaam_identificatie
    dataset = pd.merge(dataframe,
    lokaties[['waterlichaam_identificatie','stroomgebieddistrict_code','deelstroomgebieddistrict_code',
              'waterbeheerder_omschrijving','waterlichaam_type' ]],on='waterlichaam_identificatie')
    oldsize=dataset.shape[0]
    dataset=dataset.drop_duplicates()
    if verbose:
        print('  MERGE: datarecords: %d without duplicates: %d (waterbodies: %d)'
            %(oldsize,dataset.shape[0],dataset.waterlichaam_identificatie.nunique()))
        jaren=dataset['rapportagejaar'].unique()
        print('dataset for :', jaren)
        
    dataset['grootheid_code']=dataset.grootheid_code.astype('category')
    dataset['oordeelsoort_code']=dataset.oordeelsoort_code.astype('category')
    dataset['hoedanigheid_code']=dataset.hoedanigheid_code.astype('category')
        
    return dataset
###############################################################################################################################
def show_waterbodies_waterbeheerder(dataframe):
     sub=dataframe.groupby(['stroomgebieddistrict_code','waterbeheerder_omschrijving','rapportagejaar']) ['waterlichaam_identificatie'].nunique().to_frame('count').reset_index()
     display(pd.pivot_table(sub, index=['stroomgebieddistrict_code','waterbeheerder_omschrijving'], columns=['rapportagejaar'], values=['count'],
            aggfunc={'count':[np.sum]},fill_value=0))
###############################################################################################################################
def show_waterbodies_stroomgebied(dataframe):
     sub=dataframe.groupby(['stroomgebieddistrict_code','waterbeheerder_omschrijving','rapportagejaar']) ['waterlichaam_identificatie'].nunique().to_frame('count').reset_index()
     
     display(pd.pivot_table(sub, index=['stroomgebieddistrict_code'], columns=['rapportagejaar'], values=['count'],
            aggfunc={'count':[np.sum]},fill_value=0))
  
###############################################################################################################################
def draw_oordeelplot(sub1, item,show_table, selector,oordeeltype ):
            
            #generate labels based on selected data
            
            oordelen=sub1['oordeel_overall'].unique()
            
            sub1=sub1.sort_values(['rapportagejaar'])
            rapportagejaren=sorted(sub1['rapportagejaar'].unique())
            
            #sub1.rapportagejaar=sub1.rapportagejaar.astype(int)
            
            #check two versions of data
            if oordeeltype=='multi':
                omap= { 'oordeel_overall':{ 0:'slecht',1:'ontoereikend',2:'matig',3:'goed',4:'zeer goed'} }
                cols=4
            else:
                omap= { 'oordeel_overall':{ 0:'voldoet niet',1:'voldoet'}}
                cols=2
                
            #map numbers to text
            sub1=sub1.replace(omap)
            
            #generate plots for no of waterbodies and as % of all waterbodies
            a=pd.crosstab([sub1.rapportagejaar],[sub1.oordeel_overall], normalize=False)
            b=pd.crosstab([sub1.rapportagejaar],[sub1.oordeel_overall], normalize='index')  
            b=b.multiply(100)
            b=b.round({'voldoet':0,'voldoet niet':0,'zeer goed':0,'goed':0,'matig':0,'ontoereikend':0,'slecht':0})
            #rename the columnlabel of the crosstabe
            b=b.rename_axis('%', axis='columns')
            
            if show_table:
                c=pd.DataFrame(sub1.groupby(['rapportagejaar']) ['waterlichaam_identificatie'].nunique().to_frame('waterlichamen').reset_index())

                if (selector=='chemie') | (selector=='algemeen'):
                    if oordeeltype=='multi':
                        d=pd.DataFrame(sub1.groupby(['rapportagejaar']) ['typering_omschrijving'].nunique().to_frame('toetsgroepen').reset_index())
                    else:
                        d=pd.DataFrame(sub1.groupby(['rapportagejaar']) ['chemischestof_omschrijving'].nunique().to_frame('toetsgroepen').reset_index())


                c.set_index(['rapportagejaar'], inplace=True)
                       
            #add count of waterlichamen
                c=a.join(c, how='outer')
            
                if (selector=='chemie') | (selector=='algemeen'):
                    if oordeeltype=='multi':
                        d=pd.DataFrame(sub1.groupby(['rapportagejaar']) ['typering_omschrijving'].nunique().to_frame('toetsgroepen').reset_index())
                    else:
                        d=pd.DataFrame(sub1.groupby(['rapportagejaar']) ['chemischestof_omschrijving'].nunique().to_frame('toetsgroepen').reset_index())
    #                     e=pd.DataFrame(sub1.groupby(['waterbeheerder_omschrijving','waterlichaam_identificatie','rapportagejaar']) ['chemischestof_omschrijving'].nunique().to_frame('toetsgroepen').reset_index())

    #                    e=pd.pivot_table(e, index=['waterbeheerder_omschrijving','waterlichaam_identificatie'], columns=['rapportagejaar'], aggfunc=[np.sum])
    #                     display(e)
                        e=sub1[(sub1.rapportagejaar>'2014')]
                        e=pd.DataFrame(e.groupby(['waterbeheerder_omschrijving','chemischestof_omschrijving','rapportagejaar']) ['waterlichaam_identificatie'].nunique().to_frame('toetsgroepen').reset_index())
                        e=pd.pivot_table(e, index=['waterbeheerder_omschrijving','chemischestof_omschrijving'], columns=['rapportagejaar'], aggfunc=[np.sum])
                        #display(e)

                    d.set_index(['rapportagejaar'], inplace=True)

                    c=c.join(d, how='outer')
                    
                display_side_by_side(c,b)
                        
            #map colours in dataset to values in histogram
            columnnames = ['slecht','ontoereikend', 'matig', 'goed', 'zeer goed','voldoet', 'voldoet niet']
            color_list = ['red','orange', 'yellow', 'lightgreen', 'blue','lightgreen', 'red']
            colordict = dict(zip(columnnames, color_list))
            if item=='':
                item='unknown '
            
            
            label='\n waterlichamen'
            #toetsingen 
            if (selector=='chemie') | (selector=='algemeen'):
                   label='\n toetsingen'
            
            
            fig, (ax1, ax2) = plt.subplots(figsize=[15,3], ncols=2, sharey=False)    
                        
            a.plot.barh(stacked=True, width=0.8 ,
                        color=map(colordict.get,a.columns),  
                        ax=ax1, title=item +label )
            
            b.plot.barh(stacked=True, width=0.8 ,
                         color=map(colordict.get,b.columns),  
                         ax=ax2, title='\n(percentage)', xlim=[0,100] )
            
            ax1.set_yticklabels(rapportagejaren)
            ax1.set_ylabel('')
            
            ax1.set_xlabel(selector)
            ax1.legend().set_visible(False)

            ax2.set_yticklabels(rapportagejaren)
            ax2.set_ylabel('')
            #ax2.set_xlabel(selector)
            ax2.legend().set_visible(False)

            handles, labels = ax1.get_legend_handles_labels()
                                    
            fig.legend(handles, labels, loc='lower center', ncol=cols)
            #make years go down from topleft-bottomleft
            ax1.invert_yaxis()
            ax2.invert_yaxis()

            plt.show()
            
            
###############################################################################################################################
def oordeeluniform(subs):
    #create overall_oordeel based on DESK/AQUOKIT, if DESK is present it will be used if the user has choosen 'all' as method
    
    #first change categorical data into numerical ordered data necessary for the pivot!
    omap= { 'oordeel':{ 'slecht':0,'ontoereikend':1,'matig':2,'goed':3,'zeer goed':4,'voldoet niet':0,'voldoet':1} }
    sub3=subs.replace(omap)
   
    #create a pivot table with only necessary fields
    sub4=pd.pivot_table(sub3, index=['stroomgebieddistrict_code','deelstroomgebieddistrict_code',
                        'waterbeheerder_omschrijving','waterlichaam_identificatie','rapportagejaar'],
                        columns=['waardebepalingsmethode_code'],
                        values=['oordeel']).reset_index()
    
    #select if vars are present
    desk=(('oordeel','DESK') in sub4)
    aquo=(('oordeel','AQUOKIT') in sub4)
    
    if desk&aquo:
        sub4[('oordeel_overall')] = np.where(sub4[('oordeel','DESK')].isnull(),
                                             sub4[('oordeel','AQUOKIT')],sub4[('oordeel','DESK')])      
    else: 
        if aquo: 
          sub4['oordeel_overall'] =sub4['oordeel','AQUOKIT']
        if desk:
          sub4['oordeel_overall'] =sub4['oordeel','DESK']
        
    sub4.sort_index(inplace=True)    
    return sub4
###############################################################################################################################     
#selector is algemeen/chemie    
def display_stroomgebied_counts(subs, show_table, selector, gebiedselectie):
    global Usedeelstroom
    
    #subs=subs[(subs.waterbeheerder_omschrijving=="Waterschap Brabantse Delta")]
    #subs=subs[(subs.rapportagejaar=="2018")]
    
    if selector=='chemie': 
        #no of overschrijdingen / waterbeheerder en totaal aantal toetsingen remove all nan
        subsb = subs[pd.notnull(subs['chemischestof_omschrijving'])]
        oordeeltype='binair'
        set1=['voldoet','voldoet niet']
        subsb = subsb[subsb['oordeel'].isin(set1)]
        
    else:
        #no of overschrijdingen / waterbeheerder en totaal aantal toetsingen remove all nan
        subsb = subs[pd.notnull(subs['typering_omschrijving'])]
        oordeeltype='multi'
        set1=['slecht','ontoereikend', 'matig', 'goed', 'zeer goed']
        subsb = subsb[subsb['oordeel'].isin(set1)]
    
    columnnames = ['slecht','ontoereikend', 'matig', 'goed', 'zeer goed','voldoet', 'voldoet niet']
    color_list = ['red','orange', 'yellow', 'lightgreen', 'blue','lightgreen', 'red',]
    colordict = dict(zip(columnnames, color_list))
   
    subsb=subsb.rename(index=str, columns={'oordeel': 'oordeel_overall'})
    
    #only show full NL when more than 1 
        
    if gebiedselectie=='stroomgebied':
        if Usedeelstroom:
            stroomgebieden=subs['deelstroomgebieddistrict_code'].unique()
            stroomgebieden.sort()
        else:
            stroomgebieden=subs['stroomgebieddistrict_code'].unique()
            stroomgebieden.sort()
        if stroomgebieden.shape[0]>1:
            draw_oordeelplot(subsb,'Nederland',show_table,selector,oordeeltype)
        
        for item in stroomgebieden:
            if Usedeelstroom:
                subsb3=subsb[(subsb.deelstroomgebieddistrict_code==item)]
            else:
                subsb3=subsb[(subsb.stroomgebieddistrict_code==item)]
                
            if subsb.shape[0]>0:
                draw_oordeelplot(subsb3,item, show_table, selector,oordeeltype)
    
    if gebiedselectie=='waterschap':
        waterbeheerders=subs['waterbeheerder_omschrijving'].unique()
        waterbeheerders.sort()
        for item in waterbeheerders:
            sub3=subsb[(subsb.waterbeheerder_omschrijving==item)]
            draw_oordeelplot(sub3,item, show_table, selector,oordeeltype)
        

###############################################################################################################################
def display_stroomgebied_oordeel(subs, show_table, selector):
    global Usedeelstroom
    #subs=subs[(subs.waterbeheerder_omschrijving=="Waterschap Hunze en Aa's")]
    #subs=subs[(subs.rapportagejaar>"2009")]
    
    stroomgebieden=subs['stroomgebieddistrict_code'].unique()
    stroomgebieden.sort()
    
    if Usedeelstroom:
        stroomgebieden=subs['deelstroomgebieddistrict_code'].unique()
        stroomgebieden.sort()
           
    oordelen=subs['oordeel'].unique()
    #check if dataset is binair (voldoet/voldoet niet or multi)
    if (('voldoet' in oordelen) or ('voldoet niet' in oordelen)):
        oordeeltype='binair'
    else:
        oordeeltype='multi'
    
    #create single oordeel per waterbody ! ->need recoding 
    sub4=oordeeluniform(subs)
    #create table with values
    sub4.columns = sub4.columns.map(''.join)

    sub3=pd.pivot_table(sub4, index=['stroomgebieddistrict_code','deelstroomgebieddistrict_code','waterlichaam_identificatie'], columns=['rapportagejaar'], 
                         values='oordeel_overall').reset_index()

    #calculate differences between two years
    errorfound=False;
    #check if years exist
    try:
      sub3['2018-2015']=sub3['2018']-sub3['2015']
    except:
      errorfound=True  
      print('error in diffs between 2018 and 2015')
    
    if errorfound!=True: #create a histoplot   
        #remove rows with possible NaNs in diff
        sub3 = sub3[pd.notnull(sub3['2015'])]
        sub3 = sub3[pd.notnull(sub3['2018'])]
        
        #sub3 = sub3[pd.notnull(sub3['2018-2015'])]
        
        if oordeeltype=='multi':
                omap= { '2018':{ 0:'slecht',1:'ontoereikend',2:'matig',3:'goed',4:'zeer goed'} }
                cols=4
        else:
                omap= { '2018':{ 0:'voldoet niet',1:'voldoet'}}
                cols=2
        sub3=sub3.replace(omap)        
        if oordeeltype=='multi':
                omap= { '2015':{ 0:'slecht',1:'ontoereikend',2:'matig',3:'goed',4:'zeer goed'} }
                cols=4
        else:
                omap= { '2015':{ 0:'voldoet niet',1:'voldoet'}}
                cols=2
        
        
        sub3=sub3.replace(omap)
        
        sub30=pd.DataFrame()
                
        #join two dataframes outer to make sure all possible values (oordeel) are visible
        s1=pd.DataFrame(sub3['2015'].value_counts())
        s2=pd.DataFrame(sub3['2018'].value_counts())
        sub30=s1.join(s2, how='outer').fillna(0)
        
        #transpose rows/colums
        sub30=sub30.T
        #total sum of comparable waterbodies
        waterl=sub30.values.sum()//2
        
        sub3a= pd.crosstab(sub3['2015'],sub3['2018'], normalize=False)
        sub3b= pd.crosstab(sub3['2018-2015'],sub3['2015'], normalize=False)
                
        #display(sub3a)                                        
        fig,(ax,ax1)  = plt.subplots(ncols=2, figsize=[15,3])
        columnnames = ['slecht','ontoereikend', 'matig', 'goed', 'zeer goed','voldoet', 'voldoet niet']
        color_list = ['red','orange', 'yellow', 'lightgreen', 'blue','lightgreen', 'red']
        colordict = dict(zip(columnnames, color_list))
                
        sub30.plot.barh(stacked=True, width=0.8 , color=map(colordict.get,sub30.columns), title=('vergelijking 2015/2018 waterlichamen(n=%d)'%waterl),   ax=ax )
        sub3b.plot.bar(stacked=True, width=0.8 , color=map(colordict.get,sub3b.columns),
                       title='verschil 2018-2015' ,   ax=ax1 )
        display_side_by_side(sub30,sub3a,sub3b)
        plt.show()
        
#         #plot the figure
#         fig,ax  = plt.subplots( figsize=[15,3])
#         plt.title('Verschillen 2018 - 2015 (aantal waterlichamen=%d)'%sub3.shape[0])
#         #check no of bins
#         binsT=sub3['oordeel_diff'].nunique()
#         if binsT<3:
#             binsT=3
#         ax=plt.hist(sub3['oordeel_diff'],bins=binsT, rwidth=0.9)
        
#         dx=(ax[1][1]-ax[1][0])/2
#         #label the bins 
#         for i in range(binsT):
#                if ax[0][i]>0:
#                    plt.text(ax[1][i]+dx,ax[0][i],str(int(ax[0][i])),fontsize=18, color='black')
        
    
    #only show full NL when more than 1 
    print('nationaal')
    if stroomgebieden.shape[0]>1:
      draw_oordeelplot(sub4,'Nederland',show_table,selector,oordeeltype)
    print('subs')
    
    for item in stroomgebieden:
        if Usedeelstroom:
            sub3=sub4[(sub4.deelstroomgebieddistrict_code==item)]
        else:
            sub3=sub4[(sub4.stroomgebieddistrict_code==item)]
        if sub3.shape[0]>0:
                sub3.sort_index(inplace=True)
                
                draw_oordeelplot(sub3,item, show_table, selector,oordeeltype)
###############################################################################################################################
def display_waterbeheerder_oordeel(subset, show_table, selector):
    waterbeheerders=subset['waterbeheerder_omschrijving'].unique()
    waterbeheerders.sort()
    
    oordelen=subset['oordeel'].unique()
    
    if (('voldoet' in oordelen) or ('voldoet niet' in oordelen)):
        oordeeltype='binair'
    else:
        oordeeltype='multi'
    
    sub4=oordeeluniform(subset)
    for item in waterbeheerders:
            sub1=sub4[(sub4.waterbeheerder_omschrijving==item)]
            if sub1.shape[0]>0:
                 draw_oordeelplot(sub1,item,show_table, selector, oordeeltype)
###############################################################################################################################
def display_waterlichaam_oordeel(subset, show_table):
    waterlichamen=subset['waterlichaam_identificatie'].unique()
    waterlichamen.sort()
    for item in waterlichamen:
            sub1=subset[(subset.waterlichaam_identificatie==item)]
            if sub1.shape[0]>0:
                 draw_oordeelplot(sub1,item,show_table)
                    
###############################################################################################################################                    
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
    
###############################################################################################################################
def display_stroomgebied_doelen(subset, show_table):
    stroomgebieden=subset['stroomgebieddistrict_code'].unique()
    stroomgebieden.sort()

    for item in stroomgebieden:
            sub1=subset[(subset.stroomgebieddistrict_code==item)]
            sub1=sub1[['rapportagejaar','waterlichaam_identificatie',
                       'stroomgebieddistrict_code',  'chemischestof_omschrijving',
                       'typering_code','ondergrens_waarde']].drop_duplicates()
           
            display(pd.pivot_table(sub1, index=['stroomgebieddistrict_code','typering_code'], 
                                   columns=['rapportagejaar'], values=['ondergrens_waarde'], 
                                   aggfunc={'ondergrens_waarde':[np.max, np.min,np.count_nonzero]},
                                   fill_value=0))
            
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
