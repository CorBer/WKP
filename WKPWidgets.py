import WKP
from ipywidgets import widgets
from IPython.display import clear_output
import qgrid

#debug view
debug_view = widgets.Output(layout={'border': '1px solid black'})

#out = widgets.Output(layout={'border': '2px solid blue' })
#out = widgets.Output(layout=dict(height='500px', overflow_y='auto', overflow_x='auto', border='1px solid blue'))
out = widgets.Output(layout=widgets.Layout(overflow_y='scroll'))
out1= widgets.Output(layout={'border': '1px solid black'})

#declare tabs as global at start
selectedtab=0

oordeelset=[]

stofselectie = widgets.Dropdown(
    options=[''],
    value='',
    description='Parameter:',
)

groupselector=widgets.RadioButtons(
    options=['algemeen', 'chemie'],
    value='algemeen',
    description='Selectie :',
    disabled=False
)

group1selector=widgets.RadioButtons(
    options=['algemeen', 'chemie'],
    value='chemie',
    description='Stats :',
    disabled=False
)


methodselector=widgets.RadioButtons(
    options=['TT_OM', 'TT','OM'],
    value='TT_OM',
    description='Methode :',
    disabled=False
)

SGselector=widgets.RadioButtons(
    options=['all', 'NLEM','NLMS','NLRN','NLSC'],
    value='all',
    description='Stroomgebied:',
    disabled=False
)
bepalingselector=widgets.RadioButtons(
    options=['all','AQUOKIT', 'DESK'],
    value='all',
    description='Bepaling:',
    disabled=False
)
gebiedselectie = widgets.RadioButtons(
    options=['stroomgebied','waterschap'],
    value='stroomgebied',
    description='Gebiedsnivo:',
)

typeselectie = widgets.Dropdown(
    options=[''],
    value='',
    description='Watertype:',
)
#checkboxes 
checkwaterbeheer=widgets.Checkbox(
    value=False,
    description='Restrict to waterbeheerder',
    disabled=False
)

showtable=widgets.Checkbox(
    value=False,
    description='Show table with graphs',
    disabled=False
)
Usedeelstroom=widgets.Checkbox(
    value=False,
    description='Show deelstroomgebieden',
    disabled=False
)

containerleft = widgets.VBox([groupselector, stofselectie,  typeselectie, showtable])
containerright = widgets.VBox([methodselector, bepalingselector])
containermid = widgets.VBox([SGselector,gebiedselectie,Usedeelstroom])
widgetcontainer = widgets.HBox([containerleft,containermid, containerright])
fullcontainer = widgets.VBox([widgetcontainer,debug_view])
    
container1left = widgets.VBox([group1selector, typeselectie, showtable])
widget1container = widgets.HBox([container1left,containermid, containerright])
full1container = widgets.VBox([widget1container,debug_view])
           
tabs = widgets.Tab()
children=[fullcontainer, full1container ,out1]
tabs.children=children
tabs.set_title(0, 'oordeel')
tabs.set_title(1, 'counts')
tabs.set_title(2, 'dataset')
    
import ipython_memory_usage.ipython_memory_usage as imu

#main routine
@debug_view.capture(clear_output=True)
def changeview():
    global oordeelset
    
    #specific for oordeel graphs
    subset=oordeelset[(oordeelset['oordeelsoort_code']==methodselector.value)]
    
    fullsize=subset.shape[0]
    
    if bepalingselector.value!='all':
           subset=subset[(subset['waardebepalingsmethode_code']==bepalingselector.value)]
            
    if typeselectie.value!='all':
               subset=subset[(subset['waterlichaam_type']==typeselectie.value)]

    if SGselector.value!='all':
        subset=subset[(subset['stroomgebieddistrict_code']==SGselector.value)]
    
    #only for oordeel tab
    if tabs.get_title(tabs.selected_index)=="oordeel":
        if stofselectie.value!='all':
            if groupselector.value!='chemie':
              subset=subset[(subset.typering_omschrijving==stofselectie.value)]
            else:
              subset=subset[(subset.chemischestof_omschrijving==stofselectie.value)]  

    if subset.shape[0]==0:
        with out:
           clear_output(wait=True)
           print('No data available')
           return
        
    with out1:
        clear_output(wait=True)
        print('generate output')
        if groupselector.value!='chemie': 
            subset1=subset[['stroomgebieddistrict_code','waterbeheerder_omschrijving','waterlichaam_identificatie',
                            'rapportagejaar','typering_omschrijving',
                           'waardebepalingsmethode_code','oordeelsoort_code','oordeel' ]]
        else:
            subset1=subset[['stroomgebieddistrict_code','waterbeheerder_omschrijving','waterlichaam_identificatie',
                           'rapportagejaar','chemischestof_omschrijving',
                           'waardebepalingsmethode_code','oordeelsoort_code','oordeel' ]]
        qgrid_widget = qgrid.show_grid(subset1)
        display(qgrid_widget)
    
    
    with out:
        clear_output(wait=True)
        #print('selected %s %s %s [%d/%d records]'%(methodselector.value,stofselectie.value, 
        #                                       bepalingselector.value, subset.shape[0],fullsize))
        #oordeel tab
        if tabs.get_title(tabs.selected_index)=="oordeel":
            print('selected %s %s %s [%d/%d records]'%(methodselector.value,stofselectie.value, 
                                               bepalingselector.value, subset.shape[0],fullsize))
            if gebiedselectie.value=='stroomgebied':
                   WKP.display_stroomgebied_oordeel(subset, showtable.value, stofselectie.value)
            if gebiedselectie.value=='waterschap':
                   WKP.display_waterbeheerder_oordeel(subset,  showtable.value,  stofselectie.value)
        #stats tab            
        if tabs.get_title(tabs.selected_index)=="counts":
             WKP.display_stroomgebied_counts(subset, showtable.value, group1selector.value, gebiedselectie.value)
         

def selector_change(change):
    if groupselector.value!='chemie':
        stoffen=oordeelset['typering_omschrijving'].dropna().unique()
    if groupselector.value!='algemeen':
        stoffen=oordeelset['chemischestof_omschrijving'].dropna().unique()
    
    stoffen=sorted(stoffen)
    stofselectie.options=stoffen

def response(change):
       WKP.Usedeelstroom=Usedeelstroom.value 
       changeview()
    
def tabresponse(change):
      if tabs.get_title(tabs.selected_index)!="dataset":
         changeview()
    
def init():
    
    global oordeelset
    WKP.verbose=False
    oordeelset=WKP.load_merge()
    
    stoffen=oordeelset['typering_omschrijving'].dropna().unique()
    stoffen=sorted(stoffen)
    stofselectie.options=stoffen
    #oordeelset=dataset
    
    wtypes=sorted(oordeelset['waterlichaam_type'].dropna().unique())
    wtypes=sorted(wtypes)
    typeselectie.options=['all']+wtypes

    #SELECTOR RESPONSE      
    stofselectie.observe(response, names="value")
    typeselectie.observe(response, names="value")
    gebiedselectie.observe(response, names="value")
    groupselector.observe(selector_change, names="value" )
    group1selector.observe(response, names="value" )
    methodselector.observe(response, names="value" )
    SGselector.observe(response, names="value" )
    bepalingselector.observe(response, names="value" )
    showtable.observe(response, names="value")
    checkwaterbeheer.observe(response, names="value" )
    Usedeelstroom.observe(response, names="value" )
    tabs.observe(tabresponse, names="selected_index")
    
    display(tabs)
    display(out)
    selector_change(out)
    changeview()
    return oordeelset

    