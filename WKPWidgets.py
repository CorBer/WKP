import WKP
from ipywidgets import widgets
#debug view
debug_view = widgets.Output(layout={'border': '1px solid black'})

#out = widgets.Output(layout={'border': '2px solid blue' })
#out = widgets.Output(layout=dict(height='500px', overflow_y='auto', overflow_x='auto', border='1px solid blue'))
out = widgets.Output(layout=widgets.Layout(overflow_y='scroll'))

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
#main routine
@debug_view.capture(clear_output=True)
def changeview():
    
    global oordeelset
    subset=oordeelset[(oordeelset['oordeelsoort_code']==methodselector.value)]
    fullsize=subset.shape[0]
    
    if bepalingselector.value!='all':
           subset=subset[(subset['waardebepalingsmethode_code']==bepalingselector.value)]
    if typeselectie.value!='all':
           subset=subset[(subset['waterlichaam_type']==typeselectie.value)]
    if SGselector.value!='all':
        subset=subset[(subset['stroomgebieddistrict_code']==SGselector.value)]
    
    if stofselectie.value!='all':
        if groupselector.value!='chemie':
          subset=subset[(subset.typering_omschrijving==stofselectie.value)]
        else:
          subset=subset[(subset.chemischestof_omschrijving==stofselectie.value)]  
   
    print('selected %s %s %s [%d/%d records]'%(methodselector.value,stofselectie.value, 
                                               bepalingselector.value, subset.shape[0],fullsize))
    with out:
        out.clear_output()
        if subset.shape[0]==0:
           print('No data available')
           return
        
        if gebiedselectie.value=='stroomgebied':
           WKP.display_stroomgebied_oordeel(subset, showtable.value, stofselectie.value)
        if gebiedselectie.value=='waterschap':
           WKP.display_waterbeheerder_oordeel(subset,  showtable.value,  stofselectie.value)
        

def selector_change(change):
    if groupselector.value!='chemie':
        stoffen=oordeelset['typering_omschrijving'].dropna().unique()
    else:
        stoffen=oordeelset['chemischestof_omschrijving'].dropna().unique()
    
    stoffen=sorted(stoffen)
    stofselectie.options=stoffen

def response(change):
       WKP.Usedeelstroom=Usedeelstroom.value 
       changeview()
    
  
def init(dataset):
    global oordeelset
    stoffen=dataset['typering_omschrijving'].dropna().unique()
    stoffen=sorted(stoffen)
    stofselectie.options=stoffen
    oordeelset=dataset
    
    wtypes=sorted(oordeelset['waterlichaam_type'].dropna().unique())
    wtypes=sorted(wtypes)
    typeselectie.options=['all']+wtypes

    #SELECTOR RESPONSE      
    stofselectie.observe(response, names="value")
    typeselectie.observe(response, names="value")
    gebiedselectie.observe(response, names="value")
    groupselector.observe(selector_change, names="value" )
    methodselector.observe(response, names="value" )
    SGselector.observe(response, names="value" )
    bepalingselector.observe(response, names="value" )
    showtable.observe(response, names="value")
    checkwaterbeheer.observe(response, names="value" )
    Usedeelstroom.observe(response, names="value" )

    #VISUAL
    containerleft = widgets.VBox([groupselector, stofselectie,  typeselectie, showtable])
    containerright = widgets.VBox([methodselector, bepalingselector])
    containermid = widgets.VBox([SGselector,gebiedselectie,Usedeelstroom])
    widgetcontainer = widgets.HBox([containerleft,containermid, containerright])
    fullcontainer = widgets.VBox([widgetcontainer,debug_view])
    display(fullcontainer)
    display(out)
    changeview()

    