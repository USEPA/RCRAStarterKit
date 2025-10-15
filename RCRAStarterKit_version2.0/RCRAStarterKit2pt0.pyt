# -*- coding: utf-8 -*-

import requests
import pandas as pd
import arcpy
import os
from datetime import datetime
import xml.etree.cElementTree as ET
from dateutil.parser import parse

HTMBASEPATH = 'https://rcrainfo.epa.gov/rcrainfo-help/application/NationallyDefinedValues/GISModule/NDV-'
GISLOAD = r'https://rcrainfo.epa.gov/rcrainfo-help/application/ApplicationHelp/Utilities/UG-UtilitiesGISLoad.htm'

def getrequiredFields(parameters, messages):
        #get a list of required field names and descriptions
        #url = r"C:\Users\EDamico\Work\RCRAStarterKit\originals\V2\RCRAInfo-GIS-Load-Help.pdf"
        outPath = f"{parameters[1].valueAsText}\\"
        response = requests.get(GISLOAD)
        url = f"{outPath}gisload.htm"
        with open(url, "w") as file:
                file.write(response.text)
        dfs = pd.read_html(url)
        df = dfs[0]
        #reset column names to first row then drop first row
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        df_required = df[df['Required'] == 'Yes']
        requiredList = df_required['Field'].tolist()
        return requiredList
        

def getEvalutationData(epaID, badSites, messages):
    """
    Fetches evaluation data for a given EPA ID from the RCRA API and returns it as a DataFrame.
    """
    
    if epaID is None:
        return None

    try:
        xml = f'https://rcrainfo.epa.gov/webservices/rcrainfo/public/query/rcra/GetCEDataByHandler/handlerId/{epaID}'
        response = requests.get(xml)
        xml_content = response.text

        tree = ET.fromstring(xml_content)

        ns = '{http://www.exchangenetwork.net/schema/RCRA/5}'
        cafs = f'{ns}CMEFacilitySubmission'
        hid = f'{ns}EPAHandlerID'
        caEval = f'{ns}Evaluation'
        evActivityLocation = f'{ns}EvaluationActivityLocation'
        evalIdentifier = f'{ns}EvaluationIdentifier'
        evalStartDate = f'{ns}EvaluationStartDate'
        evalResponsibleAgency = f'{ns}EvaluationResponsibleAgency'
        data = []
        for event in tree.findall(cafs):
            handler = event.find(hid)
            
            for ev in event.findall(caEval):
                evalActivityLocation = ev.find(evActivityLocation)
                evalID = ev.find(evalIdentifier)
                evStartDate = ev.find(evalStartDate)
                evalAgency = ev.find(evalResponsibleAgency)
                eventData = [handler.text, evalActivityLocation.text, evalID.text, evStartDate.text,
                                 evalAgency.text]
                data.append(eventData)
            

        s = pd.Series(data)  # Convert to Series
        df = s.apply(pd.Series)

        df.columns = ['handlerId', 'EvaluationActivityLocation', 'EvaluationIdentifier',
                      'EvaluationStartDate','EvaluationResponsibleAgency' ]
        
        return df

    except Exception as e:
        badSites.append(epaID)
        messages.AddWarningMessage(f'No evaluation data found for Handler ID: {id}.  This Handler ID will not be included in the output table.')
        #print(f'An error occurred while processing Handler ID: {epaID}. Error: {e}')
        return None


def getEventData(epaID, badSites, messages):
    """
    Fetches event data for a given EPA ID from the RCRA API and returns it as a DataFrame.
    """
    
    if epaID is None:
        return None

    try:
        xml = f'https://rcrainfo.epa.gov/webservices/rcrainfo/public/query/rcra/GetCADataByHandler/handlerId/{epaID}'
        response = requests.get(xml)
        xml_content = response.text

        tree = ET.fromstring(xml_content)

        ns = '{http://www.exchangenetwork.net/schema/RCRA/5}'
        cafs = f'{ns}CorrectiveActionFacilitySubmission'
        hid = f'{ns}HandlerID'

        caArea = f'{ns}CorrectiveActionArea'
        caAuthority = f'{ns}CorrectiveActionAuthority'
        caRelEvent = f'{ns}CorrectiveActionRelatedEvent'
        caEvent = f'{ns}CorrectiveActionEvent'

        caSequence = f'{ns}EventSequenceNumber'
        caLocation = f'{ns}ActivityLocationCode'
        caAgency = f'{ns}EventAgencyCode'
        caEventCode = f'{ns}CorrectiveActionEventCode'
        #caSequence = f'{ns}EventSequenceNumber'

        caOwner = f'{ns}CorrectiveActionEventDataOwnerCode'
        data = []
        for event in tree.findall(cafs):
            handler = event.find(hid)
            # #get the event information from CorrectiveActionArea 
            # for ev in event.find(caArea).findall(caRelEvent):
            #     eventCode = ev.find(caEventCode)
            #     eventSequence = ev.find(caSequence)
            #     eventActivityLocation = ev.find(caLocation)
            #     eventAgency = ev.find(caAgency)
            #     eventOwner = ev.find(caOwner)
            #     eventData = [handler.text, eventActivityLocation.text, eventOwner.text, eventCode.text,
            #                  eventAgency.text, eventSequence.text]
            #     data.append(eventData)
            # #get the event information from CorrectiveActionAuthority
            # for ev in event.find(caAuthority).findall(caRelEvent):
            #     eventCode = ev.find(caEventCode)
            #     eventSequence = ev.find(caSequence)
            #     eventActivityLocation = ev.find(caLocation)
            #     eventAgency = ev.find(caAgency)
            #     eventOwner = ev.find(caOwner)
            #     eventData = [handler.text, eventActivityLocation.text, eventOwner.text, eventCode.text,
            #                  eventAgency.text, eventSequence.text]
            #     data.append(eventData)
            #get event information directly from base event
            for ev in event.findall(caEvent):
                eventCode = ev.find(caEventCode)
                eventSequence = ev.find(caSequence)
                eventActivityLocation = ev.find(caLocation)
                eventAgency = ev.find(caAgency)
                eventOwner = ev.find(caOwner)
                if eventCode is not None and eventSequence is not None:
                    eventData = [handler.text, eventActivityLocation.text, eventOwner.text, eventCode.text,
                                 eventAgency.text, eventSequence.text]
                    data.append(eventData)
            

        s = pd.Series(data)  # Convert to Series
        df = s.apply(pd.Series)

        df.columns = ['handlerId', 'eventActivityLocation', 'eventOwner', 'eventCode', 'eventAgency', 'eventSequence']
        
        return df

    except Exception as e:
        badSites.append(epaID)
        messages.AddWarningMessage(f'No event data found for Handler ID: {id}.  This Handler ID will not be included in the output table.')
        #print(f'An error occurred while processing Handler ID: {epaID}. Error: {e}')
        return None
    
# Function to populate the domain with coded values
def populatedomain(alist, domName, domgdb):
    for dict in alist:
        #if domain is not for a required field then allow null values
        optionalDomains = ['coordinateDataCode', 'geographicReferencePointCode', 'geometricCode','verificationMethodCode'] 
        if domName in optionalDomains: 
            dict["None"] = "Null value"    
        arcpy.management.AddCodedValueToDomain(domgdb, domName, dict['Code'], dict['Description'])

def createDomain(df, html,schemaFieldDict, hasValues_df, domgdb, messages):
    #convert dataframe to dictionary
    listofRecords = df.to_dict('records')
    
    #get description from rcra website
    fieldName = schemaFieldDict[html]
    df_val = hasValues_df[hasValues_df['Field'] == fieldName]
    description = list(df_val['Description'])[0]

    #create domain
    domName = fieldName
    existingDomains = [d.name for d in arcpy.da.ListDomains(domgdb)]
    if not domName in existingDomains:
        
        arcpy.management.CreateDomain(domgdb, domName, description, 
                                    "TEXT", "CODED")
            
        #populate domains
        populatedomain(listofRecords, domName, domgdb)

def splitFeaturebyType(df, html,hasValues_df,domgdb, schemaFieldDict,messages):
    #get a list of geometry Types
    geo_typeList = list(set(list(df['Geometry Type'])))
    for geo in geo_typeList:
        dfsel = df[df['Geometry Type'] == geo]
        listofRecords = dfsel.to_dict('records')
    
        #get description from rcra website
        fieldName = schemaFieldDict[html]
        df_val = hasValues_df[hasValues_df['Field'] == fieldName]
        description = list(df_val['Description'])[0]

        #create domain
        domName = fieldName + "_" + geo
        existingDomains = [d.name for d in arcpy.da.ListDomains(domgdb)]
        # messages.addMessage("Existing domains: " )
        # messages.addMessage(existingDomains)
        if not domName in existingDomains:
            arcpy.management.CreateDomain(domgdb, domName, description, 
                                        "TEXT", "CODED")
                
            #populate domains
            populatedomain(listofRecords, domName, domgdb)

def getSchemaFieldDict(parameters, domgdb, messages):
        #get a list of required field names and descriptions
        outPath = f"{parameters[1].valueAsText}\\"
        response = requests.get(GISLOAD)
        url = f"{outPath}gisload.htm"
        with open(url, "w") as file:
                file.write(response.text)
        dfs = pd.read_html(url)
        df = dfs[0]
        #reset column names to first row then drop first row
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        #subselect only the fields that have nationally-defined values
        hasValues_df = df[df["Description"].str.contains('nationally-defined')]
        #get a list of field names that correlate to schemas
        fldList = list(hasValues_df['Field'])

        listofSchemas = ['FeatureType', 'GISCoordinate', 'GISTierAccuracy','GISGeometric',
                        'GISGeographicReference','GISHorizontalCollection','GISVerification']
        schemaFieldDict = dict(zip(listofSchemas, fldList))
        for html in listofSchemas:
        #get the htm on web
            feat_url = f"{HTMBASEPATH}{html}.htm"
            response = requests.get(feat_url)
            #write htm to local space
            url = f"{outPath}{html}.htm"
            with open(url, "w") as file:
                file.write(response.text)
            # convert to pandas data frame
            dfs = pd.read_html(url)
            df = dfs[0]
            #reset column names to first row then drop first row
            df.columns = df.iloc[0]
            df = df.drop(df.index[0])
            if html == 'FeatureType':
                splitFeaturebyType(df, html,hasValues_df,domgdb, schemaFieldDict,messages)
            else:
                createDomain(df, html,schemaFieldDict, hasValues_df, domgdb,messages)

def buildTemplatedataset(parameters, domgdb, messages):
    #create a template dataset with the required fields and domains
    arcpy.AddMessage("Creating template dataset")
    
    #create a new feature class in the gdb
    #get geometry type from the first parameter
    desc = arcpy.Describe(parameters[0].valueAsText)
    geometryType = desc.shapeType
    #spatial_ref  = desc.spatialReference
    

    #geospatial data must use the WGS 84 (wkid = 4326) coordinate reference frame for accurate representation in RCRAInfo and associated products.
    spatial_ref = arcpy.SpatialReference(4326)  
    arcpy.management.CreateFeatureclass(domgdb, f"RCRA_Template_{geometryType}", geometryType, "","","", spatial_ref)
    #add the required fields to the feature class
    for param in parameters[1:-1]:
        if param.name != 'outputdataset':
            if param.enabled == True:
                if param.name in ['gisSequence', 'areaSequence', 'unitSequence']:
                    #if gisSequence, areaSequence, or unitSequence then set field type to Short
                    arcpy.management.AddField(f"{domgdb}\\RCRA_Template_{geometryType}", param.name, "SHORT")
                # elif param.name == 'dataCollectionDate':
                #     #if dataCollectionDate then set field type to Date
                #     arcpy.management.AddField(f"{domgdb}\\RCRA_Template_{geometryType}", param.name, "DATE")
                else:
                    #all other fields are text
                    arcpy.management.AddField(f"{domgdb}\\RCRA_Template_{geometryType}", param.name, "TEXT")
    #add horizontalReferenceCode field to the feature class
    arcpy.management.AddField(f"{domgdb}\\RCRA_Template_{geometryType}", "horizontalReferenceCode", "TEXT")
                
def appendDataset(parameters, domgdb, messages, fldsDict, nullOptionalFields):
    #append the template dataset to the input dataset
    schemaType = "NO_TEST"
    input=parameters[0].valueAsText
    desc = arcpy.Describe(input)
    geometryType = desc.shapeType
    target=f"{domgdb}\\RCRA_Template_{geometryType}"

    fieldMappings = ""
    subtype = ""
    fcList = [input]
    # Create FieldMappings object to manage merge output fields
    fieldMappings = arcpy.FieldMappings()
    # Add the target table to the field mappings class to set the schema
    fieldMappings.addTable(target)
    fldMap = arcpy.FieldMap()

    for fld in fldsDict:
        fldMap = arcpy.FieldMap()
        fldMap.addInputField(input,fldsDict[fld])
        # Set name of new output field based on the dictionary key
        hid = fldMap.outputField
        hid.name, hid.aliasName, hid.type = fld, fld, "TEXT"
        fldMap.outputField = hid
        # Add output field to field mappings object
        fieldMappings.addFieldMap(fldMap)

    arcpy.management.Append(fcList, target, schemaType, 
                        fieldMappings, subtype)
    
    #update horizontalReferenceCode field to be 003
    with arcpy.da.UpdateCursor(target, ["horizontalReferenceCode"]) as cursor:
        for row in cursor:
            row[0] = "003"
            cursor.updateRow(row)
    
    #convert dataCollectionDate field to correct date format but keeping as a text field
    fld = 'dataCollectionDate'
    if fld in [f.name for f in arcpy.ListFields(target)]:
        
        with arcpy.da.UpdateCursor(target, [fld], f'{fld} is not null') as cursor:
            for row in cursor:
                #get the date format
                date_object = parse(row[0])
                #convert to string to format Y-m-d and update the row
                row[0] = date_object.strftime('%Y-%m-%d')
                cursor.updateRow(row)   
        

    #remove extraneous "out" fields if necessary
    outFldList =[f.name for f in arcpy.ListFields(target) if f.name.startswith("out")]
    if outFldList:
        arcpy.management.DeleteField(target, outFldList)

    #remove non-required fields not selected by user  
    if nullOptionalFields:
        arcpy.management.DeleteField(target, nullOptionalFields)

    #rename the output dataset to the name of the input dataset
    if len(desc.name) <2:
        outName = f"{desc.aliasName}_updated".replace(" ","_")
    else:
        outName = f"{desc.name}_updated".replace(" ","_")
    #check to see if the output name already exists
    #messages.addMessage(f"{domgdb}//{outName}")
    if arcpy.Exists(f"{domgdb}//{outName}"):
        #if it does then add the date/time to the name (eventually might want to change this to overwrite output instead)
        current_datetime = datetime.now().strftime("%m%d%Y_%H%M")
        outName = f"{outName}_{current_datetime}"
    
    arcpy.management.Rename(target, outName)
    
    
    return(outName)
class Toolbox:
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "RCRA Boundary Schema Alignment Toolbox"
        self.alias = "RCRA Boundary Toolkit"

        # List of tool classes associated with this toolbox
        self.tools = [Tool, Tool_Event, Tool_EvaluationData, Tool_GenerateGeoJSON]

class Tool_EvaluationData:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "3 - Get Evaluation Data"
        self.description = "Grabs Evaluation data from RCRAInfo"
    def getParameterInfo(self):
        #Define parameter definitions
        #Identify the required parameters for the tool
        # Select RCRA data layer parameter
        param0 = arcpy.Parameter(
            displayName="Select Aligned RCRA data layer",
            name="in_features",
            datatype="GPFeatureLayer",
            parameterType="Required",
            #category = "Required",
            direction="Input")
        # param1 = arcpy.Parameter(
        #     displayName="Handler ID",
        #     name="HandlerID",
        #     datatype="Field",
        #     parameterType="Optional",
        #     category = "Required Fields",
        #     direction="Input")
        # param1.filter.list = ["Text"]
        # param1.value = ""
        # param1.parameterDependencies = [param0.name]
        #add derived output parameter
        param1 = arcpy.Parameter(
        displayName="Output Table",
        name="out_table",
        datatype="Table",
        parameterType="Derived",
        direction="Output")
        params = [param0, param1]
        #params = nogroupParams + requiredparams + optionalparams
        return params
    
    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
  

        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return
   

    def execute(self, parameters, messages, rcraDataLayer=None):
            """The source code of the tool."""
            if not rcraDataLayer:
                rcraDataLayer = parameters[0].valueAsText
            
            #get a list of handler IDs
            handlerIDs = [row[0].strip() for row in arcpy.da.SearchCursor(rcraDataLayer, ['handlerId'], "handlerId IS NOT NULL")]
            badSites = []
            # Create a DataFrame to store the results
            cols = ['handlerId', 'EvaluationActivityLocation', 'EvaluationIdentifier', 'EvaluationStartDate', 'EvaluationResponsibleAgency']
            eval_df = pd.DataFrame(columns=cols)
            messages.addMessage("Starting Get Evaluation Data tool")
            nodataList = []
            for handlerId in handlerIDs:  
                # Call the function with the handler ID
                data_df = getEvalutationData(handlerId, badSites, messages)
                #append the data to the DataFrame
                if not data_df.empty:
                    eval_df = pd.concat([eval_df, data_df], ignore_index=True)
                else:
                    nodataList.append(handlerId)
                    #messages.AddMessage(f'No Evaluation data found for Handler ID: {handlerId}')  
            if nodataList:
                messages.AddWarningMessage(f'No evaluation data found for Handler IDs: {", ".join(nodataList)}.  These Handler IDs will not be included in the output table.')
            #get base path of rcraDataLayer
            dataPath = arcpy.Describe(rcraDataLayer).catalogPath
            parent_path = os.path.dirname(dataPath)
            if parent_path.endswith(".gdb"):
                #if .gdb is in the path then go to the folder above it
                outPath = parent_path
                parent_path = os.path.dirname(parent_path)
                #check to see if the parent path in outPath
                #if parent_path in outPath:
                # outPath = os.path.basename(outPath)
            else:
                outPath = parent_path


            rcraName = arcpy.Describe(rcraDataLayer).name
            output_csv = f"{parent_path}\\{rcraName}_nodups.csv"
            # # Save the DataFrame to a CSV file
        
            eval_df.drop_duplicates().to_csv(output_csv, columns= cols, header=True, index=False)
            #Save csv to a file geodatabase
            arcpy.management.CopyRows(output_csv, f'{outPath}\\{rcraName}_Evaluation')
            if arcpy.Exists(f'{outPath}\\{rcraName}_Evaluation'):
                messages.addMessage(f"Evaluation data successfully saved to {outPath}\\{rcraName}_Evaluation")
                arcpy.Delete_management(output_csv)
            arcpy.SetParameterAsText(1, f"{outPath}\\{rcraName}_Evaluation")
class Tool_Event:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "2 - Get Event Data"
        self.description = "Grabs event data from RCRAInfo"
    def getParameterInfo(self):
        #Define parameter definitions
        #Identify the required parameters for the tool
        # Select RCRA data layer parameter
        param0 = arcpy.Parameter(
            displayName="Select Aligned RCRA data layer",
            name="in_features",
            datatype="GPFeatureLayer",
            parameterType="Required",
            #category = "Required",
            direction="Input")
        # param1 = arcpy.Parameter(
        #     displayName="Handler ID",
        #     name="HandlerID",
        #     datatype="Field",
        #     parameterType="Optional",
        #     category = "Required Fields",
        #     direction="Input")
        # param1.filter.list = ["Text"]
        # param1.value = ""
        # param1.parameterDependencies = [param0.name]
        #add derived output parameter
        param1 = arcpy.Parameter(
        displayName="Output Table",
        name="out_table",
        datatype="Table",
        parameterType="Derived",
        direction="Output")
        params = [param0, param1]
        #params = nogroupParams + requiredparams + optionalparams
        return params
    
    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        # if parameters[0].altered:
        #     parameters[1].enabled = True
        #     #parameters[1].value = "Enter Handler ID" 
        #     parameters[1].setWarningMessage("This field is required")


        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return
    
    

    def execute(self, parameters, messages, rcraDataLayer=None):
        """The source code of the tool."""
        if not rcraDataLayer:
            rcraDataLayer = parameters[0].valueAsText
           
        #get a list of handler IDs
        handlerIDs = [row[0].strip() for row in arcpy.da.SearchCursor(rcraDataLayer, ['handlerId'], "handlerId IS NOT NULL")]
        badSites = []
        # Create a DataFrame to store the results
        cols = ['handlerId', 'eventActivityLocation', 'eventOwner', 'eventCode', 'eventAgency', 'eventSequence']
        eval_df = pd.DataFrame(columns=cols)
        messages.addMessage("Starting Get Event Data tool")
        nodataList = []
        for handlerId in handlerIDs:  
            # Call the function with the handler ID
            data_df = getEventData(handlerId, badSites, messages)
            #append the data to the DataFrame
            if not data_df.empty:
                eval_df = pd.concat([eval_df, data_df], ignore_index=True)
            else:
                
                nodataList.append(handlerId)
               
        if nodataList:
            messages.AddWarningMessage(f'No Event data found for Handler IDs: {", ".join(nodataList)}.  These Handler IDs will not be included in the output table.')
        #get base path of rcraDataLayer
        dataPath = arcpy.Describe(rcraDataLayer).catalogPath
        parent_path = os.path.dirname(dataPath)
        if parent_path.endswith(".gdb"):
            #if .gdb is in the path then go to the folder above it
            outPath = parent_path
            parent_path = os.path.dirname(parent_path)

        else:
            outPath = parent_path


        rcraName = arcpy.Describe(rcraDataLayer).name
        output_csv = f"{parent_path}\\{rcraName}_nodups.csv"
        # # Save the DataFrame to a CSV file
        eval_df.drop_duplicates().to_csv(output_csv, columns= cols, header=True, index=False)
        #Save csv to a file geodatabase
        arcpy.management.CopyRows(output_csv, f'{outPath}\\{rcraName}_Events')
        if arcpy.Exists(f'{outPath}\\{rcraName}_Events'):
            messages.addMessage(f"Event data successfully saved to {outPath}\\{rcraName}_Events")
            arcpy.Delete_management(output_csv)
        arcpy.SetParameterAsText(1, f"{outPath}\\{rcraName}_Events")

class Tool:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "1 - Update to RCRA Schema"
        self.description = "Updates RCRA data to the RCRA schema"
        

    def getParameterInfo(self):
    #Define parameter definitions
    #Identify the required parameters for the tool
    # Select RCRA data layer parameter
        param0 = arcpy.Parameter(
            displayName="Select RCRA data layer",
            name="in_features",
            datatype="GPFeatureLayer",
            parameterType="Required",
            #category = "Required",
            direction="Input")
        
        #param0.value = "Enter RCRA Layer"
        #param0.filter.list = ["Polygon"]
    
    #Get location of output folder where schema corrected dataset will reside
        param1 = arcpy.Parameter(
            displayName="Output Folder",
            name="outputdataset",
            datatype="DEFolder",
            parameterType="Required",
            #category = "Required Fields",
            direction="Input")
        param1.value = ""
        param1.parameterDependencies = [param0.name]
        param1.filter.list = ["Text"]
        
    # Select Handler ID parameter
        param2 = arcpy.Parameter(
            displayName="Handler ID",
            name="handlerId",
            datatype="Field",
            parameterType="Optional",
            category = "Required Fields",
            direction="Input")
        param2.filter.list = ["Text"]
        param2.value = ""
        param2.parameterDependencies = [param0.name]

        
    # Select GIS Owner parameter
        param3 = arcpy.Parameter(
            displayName="GIS Owner",
            name="gisOwner",
            datatype="Field",
            parameterType="Optional",
            category = "Required Fields",
            direction="Input")

        param3.value = ""
        param3.parameterDependencies = [param0.name]
        param3.filter.list = ["Text"]

    # Select Feature Type Code parameter
        param4 = arcpy.Parameter(
            displayName="Feature Type Code",
            name="featureTypeCode",
            datatype="Field",
            parameterType="Optional",
            category = "Required Fields",
            direction="Input")

        param4.value = ""
        param4.parameterDependencies = [param0.name]
        param4.filter.list = ["Text"]
        
    # Select the Tier Accuracy Code parameter
        param5 = arcpy.Parameter(
            displayName="Tier Accuracy Code",
            name="tierAccuracyCode",
            datatype="Field",
            parameterType="Optional",
            category = "Required Fields",
            direction="Input")

        param5.value = ""
        param5.parameterDependencies = [param0.name]
        param5.filter.list = ["Text"]

      
    # Select the Horizontal Collection Code parameter
        param6 = arcpy.Parameter(
            displayName="Horizontal Collection Code",
            name="horizontalCollectionCode",
            datatype="Field",
            parameterType="Optional",
            category = "Required Fields",
            direction="Input")

        param6.value = ""
        param6.parameterDependencies = [param0.name]
        param6.filter.list = ["Text"]

        #Note gisSequence is required but will be updated 
        #automatically if it is not included by the user
        param7 = arcpy.Parameter(
            displayName="GIS Sequence",
            name="gisSequence",
            datatype="Field",
            parameterType="Optional",
            category = "Required Fields",
            direction="Input")

        param7.value = ""
        param7.parameterDependencies = [param0.name]
        param7.filter.list = ["Text", "Double", "Long", "Short"]

        #Horizontal Reference Code is required but will be updated automatically because RCRA requires it
        #to be in WGS 84 coordinate reference
   

        nogroupParams = [param0, param1]
        #Add the required parameters to the list of parameters
        requiredparams = [param2, param3,param4, param5, param6, param7]
        #Add the optional parameters to the list of parameters
        # Select the Horizaontal Collection Code parameter
        param8 = arcpy.Parameter(
            displayName="Data Collection Date",
            name="dataCollectionDate",
            datatype="Field",
            parameterType="Optional",
            category = "Optional",
            direction="Input")

        param8.value = ""
        param8.parameterDependencies = [param0.name]
        param8.filter.list = ["Text", "Date"]
        param8.value = ""

        #Select Coordinate Data Code parameter
        param9 = arcpy.Parameter(
            displayName="Coordinate Data Code",
            name="coordinateDataCode",
            datatype="Field",
            parameterType="Optional",
            category = "Optional",
            direction="Input")

        param9.value = ""
        param9.parameterDependencies = [param0.name]
        param9.filter.list = ["Text"]
        param9.value = ""

        #Select Geographic Reference Point Code parameter
        param10 = arcpy.Parameter(
            displayName="Geographic Reference Point Code",
            name="geographicReferencePointCode",
            datatype="Field",
            parameterType="Optional",
            category = "Optional",
            direction="Input")

        param10.value = ""
        param10.parameterDependencies = [param0.name]
        param10.filter.list = ["Text"]
        param10.value = ""

        #Select Geometric Code parameter
        param11 = arcpy.Parameter(
            displayName="Geometric Code",
            name="geometricCode",
            datatype="Field",
            parameterType="Optional",
            category = "Optional",
            direction="Input")

        param11.value = ""
        param11.parameterDependencies = [param0.name]
        param11.filter.list = ["Text"]
        param11.value = ""

        #Select Verification Method Code parameter
        param12 = arcpy.Parameter(
            displayName="Verification Method Code",
            name="verificationMethodCode",
            datatype="Field",
            parameterType="Optional",
            category = "Optional",
            direction="Input")

        param12.value = ""
        param12.parameterDependencies = [param0.name]
        param12.filter.list = ["Text"]
        param12.value = ""

        #Select the optional name or identifier of the feature
        param13 = arcpy.Parameter(
            displayName="Feature Name",
            name="featureName",
            datatype="Field",
            parameterType="Optional",
            category = "Optional",
            direction="Input")

        param13.value = ""
        param13.parameterDependencies = [param0.name]
        param13.filter.list = ["Text"]
        param13.value = ""
        
        #Select the optional notes or comments about the feature
        param14 = arcpy.Parameter(
            displayName="notes",
            name="notes",
            datatype="Field",
            parameterType="Optional",
            category = "Optional",
            direction="Input")

        param14.value = ""
        param14.parameterDependencies = [param0.name]
        param14.filter.list = ["Text"]
        param14.value = ""

        # Select the optional unitSequence parameter    
        param15 = arcpy.Parameter(
            displayName="Unit Sequence",
            name="unitSequence",
            datatype="Field",
            parameterType="Optional",
            category = "Optional",
            direction="Input")
        param15.value = ""
        param15.parameterDependencies = [param0.name]
        param15.filter.list = ["Double", "Long", "Short"]
        param15.value = ""
        
        # Select the optional areaSequence parameter    
        param16 = arcpy.Parameter(
            displayName="Area Sequence",
            name="areaSequence",
            datatype="Field",
            parameterType="Optional",
            category = "Optional",
            direction="Input")
        param16.value = ""
        param16.parameterDependencies = [param0.name]
        param16.filter.list = ["Double", "Long", "Short"]
        param16.value = ""

        #add derived output parameter
        param17 = arcpy.Parameter(
        displayName="Output Features",
        name="out_features",
        datatype="GPFeatureLayer",
        parameterType="Derived",
        direction="Output")

        param18 = arcpy.Parameter(
        displayName="Output Event Table",
        name="out_eventtable",
        datatype="Table",
        parameterType="Derived",
        direction="Output")

        param19 = arcpy.Parameter(
        displayName="Output Evaluation Table",
        name="out_evaltable",
        datatype="Table",
        parameterType="Derived",
        direction="Output")

        optionalparams = [param8, param9, param10, param11, param12, param13, param14, param15, param16, param17, param18, param19]
        params = nogroupParams + requiredparams + optionalparams
        return params
        
    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        if parameters[0].altered:
            parameters[1].enabled = True
            #parameters[1].value = "Enter Handler ID" 
            parameters[1].setWarningMessage("This field is required")


        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return
    

    

    def execute(self, parameters, messages):
        """The source code of the tool."""
        

        #check to see if any of the required fields are null
        fieldswithNulls = []
        notEntered = []
        for param in parameters[2:8]:
            if param.valueasText is not None:
                fld = [param.valueAsText][0]
                messages.addMessage("Checking for null values in required field: " + fld)
                nullList = arcpy.management.SelectLayerByAttribute(parameters[0].valueAsText, 
                                                            "NEW_SELECTION", fld + " IS NULL")
                nullCount = int(arcpy.management.GetCount(nullList).getOutput(0))
                
                #clear the selection
                arcpy.management.SelectLayerByAttribute(parameters[0].valueAsText, 
                                                            "CLEAR_SELECTION")
                if nullCount > 0:
                        fieldswithNulls.append(fld)
            else:
                notEntered.append(param.displayName)
        
        #check non-required fields for nulls
        for param in parameters[8:16]:
            if param.valueasText is not None:
                fld = [param.valueAsText][0]
                messages.addMessage("Checking for null values in optional field: " + fld)
                nullList = arcpy.management.SelectLayerByAttribute(parameters[0].valueAsText, 
                                                            "NEW_SELECTION", fld + " IS NULL")
                nullCount = int(arcpy.management.GetCount(nullList).getOutput(0))
                
                #clear the selection
                arcpy.management.SelectLayerByAttribute(parameters[0].valueAsText, 
                                                            "CLEAR_SELECTION")
                if nullCount > 0:
                        fieldswithNulls.append(fld)
       
        
        if fieldswithNulls:
            #for f in fieldswithNulls:
            messages.addWarningMessage("There are null values in the field(s): " + ", ".join(fieldswithNulls) + ". Nulls need to be updated before json can be created.")
        
        for val in notEntered:
            #if the field is not selected then add a warning message    
            messages.AddWarningMessage( f"No field was selected for required field {val}.  This field will be set up but it will need to be populated before json can be created.")
                #return
        if notEntered:
            msg = """json:
                [{"element": "content",
                "data": ["Check out this link for more help and information about updating these fields: ", 
                        {"element": "hyperlink", 
                            "data": "RCRA GIS Load Help", 
                            "link": "https://rcrainfo.epa.gov/rcrainfo-help/application/index.htm#t=ApplicationHelp%2FUtilities%2FUG-UtilitiesGISLoad.htm"}]}]"""
            messages.addMessage(msg)
        
        messages.addMessage("Setting up domains")
        schemaFieldDict = {}
        gdbfolder = parameters[1].valueAsText
        gdbPath = gdbfolder
        #outPath = os.path.join(gdbPath, 'htm')
        # print(gdbPath)
        gdbName = "RCRA_Updated_Schema"
        domgdb = gdbPath + "\\" + gdbName + '.gdb'
        #check to see if gdb already exists, if not create gdb
        if not arcpy.Exists(domgdb):
            arcpy.CreateFileGDB_management(gdbPath,gdbName)
            messages.addMessage(f"New GDB created: {domgdb}") 
        else:
            messages.addMessage(f"GDB already exists, data will be added to: {domgdb}")
        getSchemaFieldDict(parameters, domgdb,messages)
        
        messages.addMessage("Domains are complete.  Building template dataset")
        
        #create a template dataset with the required fields and domains as well as any optional fields entered
        buildTemplatedataset(parameters, domgdb, messages)
    
        #create a dictionary of parameters and match them to the fields in the template dataset
        fldsDict = {}
        if parameters[2].valueasText is not None:
            fldsDict['handlerId'] = parameters[2].valueAsText
        if parameters[3].valueasText is not None:
            fldsDict['GISOwner'] = parameters[3].valueAsText
        if parameters[4].valueasText is not None:    
            fldsDict['FeatureTypeCode'] = parameters[4].valueAsText
        if parameters[5].valueasText is not None:
            fldsDict['TierAccuracyCode'] = parameters[5].valueAsText
        if parameters[6].valueasText is not None:
            fldsDict['HorizontalCollectionCode'] = parameters[6].valueAsText
        if parameters[7].valueasText is not None:
            fldsDict['GISSequence'] = parameters[7].valueAsText
        nullOptionalFields = []
        #check if any of the optional fields are selected and add them to the dictionary
        if parameters[8].valueasText is not None:
            fldsDict['DataCollectionDate'] = parameters[8].valueAsText
        else:
            nullOptionalFields.append('DataCollectionDate')
        if parameters[9].valueasText is not None: 
            fldsDict['CoordinateDataCode'] = parameters[9].valueAsText
        else:
            nullOptionalFields.append('CoordinateDataCode')
            
        if parameters[10].valueasText is not None:
            fldsDict['geographicReferencePointCode'] = parameters[10].valueAsText
        else:
            nullOptionalFields.append('geographicReferencePointCode')

        if parameters[11].valueasText is not None:
            fldsDict['GeometricCode'] = parameters[11].valueAsText
        else:
            nullOptionalFields.append('GeometricCode')

        if parameters[12].valueasText is not None:    
            fldsDict['VerificationMethodCode'] = parameters[12].valueAsText
        else:
            nullOptionalFields.append('VerificationMethodCode')

        if parameters[13].valueasText is not None:    
            fldsDict['FeatureName'] = parameters[13].valueAsText    
        else:
            nullOptionalFields.append('FeatureName')

        if parameters[14].valueasText is not None:
            fldsDict['Notes'] = parameters[14].valueAsText
        else:
            nullOptionalFields.append('Notes')
        
        #append the template dataset to the input dataset
        outputDataLayer = appendDataset(parameters, domgdb, messages, fldsDict, nullOptionalFields)
        messages.addMessage(f"Append completed.  ")
        messages.addMessage(f"Renaming output dataset to {outputDataLayer}")

        #add the domains to the output dataset
        #get a list of the domains in the gdb
        domains =[d.name for d in arcpy.da.ListDomains(domgdb) if not d.name.startswith('featureTypeCode')]
        desc = arcpy.Describe(f"{domgdb}//{outputDataLayer}")
        shapeType = desc.shapeType
        # domains.append(f"FeatureTypeCode_{shapeType}")
        #remove null optional fields from the domains
        messages.addMessage(f"Null optional fields: {', '.join(nullOptionalFields)}")
        if nullOptionalFields:     
            for fld in nullOptionalFields:
                if fld in domains:
                    domains.remove(fld)
                    messages.addMessage(f"Domains to be removed: {fld}")

        for dom in domains:
            #add the domain to the output dataset
            arcpy.management.AssignDomainToField(f"{domgdb}//{outputDataLayer}", dom, dom)
            messages.addMessage(f"Domain {dom} added to {dom} field")   
        #add the domain to the output dataset
        #check to see if the shapeType is Point, if so change it to Points
        if shapeType == "Point":
            shapeType = "Points"
        arcpy.management.AssignDomainToField(f"{domgdb}//{outputDataLayer}", "FeatureTypeCode", f"FeatureTypeCode_{shapeType}") 
        messages.addMessage(f"Domain FeatureTypeCode added to FeatureTypeCode field")       
        #get a list of the fields in the output dataset
        arcpy.SetParameterAsText(15, f"{domgdb}//{outputDataLayer}")

           

        #If handlerid field is not empty then run the getEventData function and the getEvaluationData function
        if parameters[2].valueAsText is not None:
            messages.addMessage("Running Get Event Data tool")
            #run the getEventData tool
            tool = Tool_Event()
            tool.execute([f"{domgdb}//{outputDataLayer}"], messages, f"{domgdb}//{outputDataLayer}")
            arcpy.SetParameterAsText(16, f"{domgdb}\\{outputDataLayer}_Events")
            
            messages.addMessage("Running Get Evaluation Data tool")
            toolEval = Tool_EvaluationData()
            toolEval.execute([f"{domgdb}//{outputDataLayer}"], messages, f"{domgdb}//{outputDataLayer}")
            arcpy.SetParameterAsText(17, f"{domgdb}\\{outputDataLayer}_Evaluation")
            

        else:
            messages.addMessage("No Handler ID field selected.  Skipping Get Event Data and Evaluation tools.")
        # #return

        messages.addMessage(f"The updated dataset can be found: {domgdb}\\{outputDataLayer}.")
        msg = """json:
            [{"element": "content",
            "data": ["Check out this link for more help and information: ", 
                    {"element": "hyperlink", 
                        "data": "RCRA GIS Load Help", 
                        "link": "https://rcrainfo.epa.gov/rcrainfo-help/application/index.htm#t=ApplicationHelp%2FUtilities%2FUG-UtilitiesGISLoad.htm"}]}]"""
        #add a message to the messages object           
        messages.AddMessage(msg)    
    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return

class Tool_GenerateGeoJSON:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "4 - Generate GeoJSON"
        self.description = "Checks that the required fields are populated and creates a GeoJSON file"
    def getParameterInfo(self):
        #Define parameter definitions
        #Identify the required parameters for the tool
        # Select RCRA data layer parameter
        param0 = arcpy.Parameter(
            displayName="Select Aligned RCRA data layer",
            name="in_features",
            datatype="GPFeatureLayer",
            parameterType="Required",
            #category = "Required",
            direction="Input")
           #Get location of output folder where schema corrected dataset will reside
        param1 = arcpy.Parameter(
            displayName="Output Folder",
            name="outputdataset",
            datatype="DEFolder",
            parameterType="Required",
            #category = "Required Fields",
            direction="Input")
        param1.value = ""
        param1.parameterDependencies = [param0.name]
        param1.filter.list = ["Text"]
        
        params = [param0, param1]
        #params = nogroupParams + requiredparams + optionalparams
        return params
    
    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
  

        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return
   

    def execute(self, parameters, messages, rcraDataLayer=None):
        """The source code of the tool."""
        if not rcraDataLayer:
            rcraDataLayer = parameters[0].valueAsText
            
            #get a list of the required fields
        requiredFields = [f.upper() for f in getrequiredFields(parameters, messages)]
        #Check that all of the required fields are populated
        rcraDataFields = [f.name.upper() for f in arcpy.ListFields(rcraDataLayer)]
        missingFields = []
        nullFields = []
        for fld in requiredFields:
            if fld not in rcraDataFields:
                missingFields.append(fld)
                #messages.AddErrorMessage(f'The field {fld} is not present in the input layer. Please ensure that the input layer contains all of the required fields.')
                #print(f'The field {fld} is not present in the input layer. Please ensure that the input layer contains all of the required fields.')
                #return
            else:
                nullCheck = [row[0] for row in arcpy.da.SearchCursor(rcraDataLayer, fld, fld + " IS NULL")]
                if len(nullCheck) > 0:
                    nullFields.append(fld)
                    # messages.AddWarningMessage(f'The field {fld} is not populated for some records in the input layer. Please ensure that all required fields are populated.')
                    # print(f'The field {fld} is not populated for some records in the input layer. Please ensure that all required fields are populated.')
                    # return
        #check if any non required fields are null
        non_requiredNulls = []
        nonRequiredFields = [f.name.upper() for f in arcpy.ListFields(rcraDataLayer) if f.name.upper() not in requiredFields]
        for fld in nonRequiredFields:
            nullCheck = [row[0] for row in arcpy.da.SearchCursor(rcraDataLayer, fld, fld + " IS NULL")]
            if len(nullCheck) > 0:
                #if nulls are found then update that field to a blank string
                # with arcpy.da.UpdateCursor(rcraDataLayer, fld, fld + " IS NULL") as cursor:
                #     for row in cursor:
                #         row[0] = ""
                #         cursor.updateRow(row)
                non_requiredNulls.append(fld) 
        msg = """json:
                [{"element": "content",
                "data": ["Check out this link for more help and information about updating these fields: ", 
                        {"element": "hyperlink", 
                            "data": "RCRA GIS Load Help", 
                            "link": "https://rcrainfo.epa.gov/rcrainfo-help/application/index.htm#t=ApplicationHelp%2FUtilities%2FUG-UtilitiesGISLoad.htm"}]}]"""
        
        if not missingFields and not nullFields and not non_requiredNulls:
            messages.AddMessage("All required fields are present and populated.  Proceeding to create GeoJSON file.")
            #Create a GeoJSON file
            outputFolder = parameters[1].valueAsText
            outputFile= f'{outputFolder}\\{rcraDataLayer}.geojson'
            #outputFile = os.path.join(outputFolder, 'rcra_data.geojson')
            if arcpy.Exists(outputFile):
                arcpy.management.Delete(outputFile) 
            
            arcpy.conversion.FeaturesToJSON(rcraDataLayer, outputFile, "FORMATTED", "NO_Z_VALUES", "NO_M_VALUES", "GEOJSON")
            messages.AddMessage(f'{rcraDataLayer}.geojson file created at {outputFile}')
        else:
            
            missing = 0 
            null = 0   
            if missingFields and nullFields:
                messages.AddErrorMessage(f'The following required fields are missing from the input layer:\n{", ".join(missingFields)}. \nPlease ensure that the input layer contains all of the required fields.')
                messages.AddErrorMessage(f'The following required fields are missing some records in the input layer:\n{", ".join(nullFields)}. \nPlease ensure that all required fields are populated.')
                messages.addMessage(msg)
                missing = 1
                null = 1
                return
            elif nullFields:
                messages.AddErrorMessage(f'The following required fields are missing some records in the input layer:\n{", ".join(nullFields)}. \nPlease ensure that all required fields are populated.')
                messages.addMessage(msg)
                null = 1
                return
            elif missingFields:
                messages.AddErrorMessage(f'The following required fields are missing from the input layer:\n{", ".join(missingFields)}. \nPlease ensure that the input layer contains all of the required fields.')
                messages.addMessage(msg)
                missing = 1
                return
        # else:
            # messages.AddMessage("All required fields are present and populated.  Proceeding to create GeoJSON file.")
            
            if non_requiredNulls:
                messages.AddWarningMessage(f'The following non-required fields contain nulls in the input layer:\n{", ".join(non_requiredNulls)}. \nPlease ensure that the fields do not contain any nulls.')
                messages.addMessage(msg)
                null = 1
            
            
            if missing > 0 and null > 0:
                        messages.AddErrorMessage("There are missing or null fields.  Please fix these issues before creating a GeoJSON file.")
            elif missing > 0:
                messages.AddErrorMessage("There are missing required fields.  Please fix these issues before creating a GeoJSON file.")
            elif null > 0:
                messages.AddErrorMessage("There are null values found.  Please fix these issues before creating a GeoJSON file.")

                

