################################
# To be used in conjunction with the model creator. Assumes that all the fields are in the same location as how the model creates them.
# Version 1.1.0
################################
import arcpy
import os
import requests
import sys
from arcpy import env


## Python 3

def getAttributes(epaID, badSites):
    ## only triggers if epaID is not null
    if epaID is not None:

        ## url constructed with the epaID
        url = 'https://ofmpub.epa.gov/apex/cimc_dws/cimc_patdws_apex/GET/CIMCWS/'+ epaID
        # arcpy.AddMessage(epaID)
        ## open JSON and return data
        try:
            ## get request for the web service
            r = requests.get(url)
            siteJSON = r.json()
            return siteJSON
        ## if no JSON returned, then add epaID to log
        except requests.exceptions.Timeout as e:
            arcpy.AddMessage('Connection to web service for site '+epaID+' timed out')
        except:
            if epaID not in badSites:
                arcpy.AddMessage('Site '+epaID+' not found in the RCRAInfo web service')
                badSites.append(epaID)

    else:
        arcpy.AddMessage('A site has a null Handler_ID field. Please check the input feature class. ')

## input feature class
# allFC = arcpy.GetParameterAsText(0)
# fcList = allFC.rsplit(';')
# outClassLocation = arcpy.GetParameterAsText(1)

## for running from command line
allFC = sys.argv[1]
fcList = allFC.rsplit(';')
outClassLocation = sys.argv[2]


for fc in fcList:
    outputName = fc.rsplit('\\')[-1] + "_skoutput"
    arcpy.AddMessage('Creating output: ' + outputName)
    # print('Creating output: ' + outputName)
    ## counter for bad epaIDs
    badSites = []
    geoType = arcpy.Describe(fc).shapeType

    ## reset ssl context to bypass ssl issues. Required when pinging non-production web service
    # ssl._create_default_https_context = ssl._create_unverified_context

    ## if/else block to determine where the log needs to be placed (directly outside gdb)
    if arcpy.Describe(os.path.dirname(fc)).dataType == 'FeatureDataset':
        # arcpy.AddMessage('in a FeatureDataset')
        logFolder = os.path.dirname(os.path.dirname(os.path.dirname(fc)))
        # outClassLocation = arcpy.SetParameterAsText(1,os.path.dirname(os.path.dirname(os.path.dirname(fc))))
    else:
        # arcpy.AddMessage('not in a FeatureDataset')
        logFolder = os.path.dirname(os.path.dirname(fc))
        # outClassLocation = arcpy.SetParameterAsText(1,os.path.dirname(os.path.dirname(fc)))
    # arcpy.AddMessage('feature class geometry type: ' + arcpy.Describe(fc).shapeType)
    ## create log document and write initial line
    log = open(logFolder+'/RCRAscriptlog.txt','w+')
    log.write('SUMMARY OF SITES: \n')
    ## create temporary in_memory feature class to store records for event controls. Copy input feature class to maintain domains
    ## and then delete records. Will eventually be written to output location.
    outClass = arcpy.FeatureClassToFeatureClass_conversion(fc, 'in_memory','inMemory_Output')
    arcpy.DeleteFeatures_management(outClass)

    inputCopy = arcpy.FeatureClassToFeatureClass_conversion(fc, 'in_memory','inMemory_input')

    fieldArray = ['EPA_PROGRAM', 'REGION', 'HANDLER_ID', 'HANDLER_NAME', 'FED_WASTE_GENERATOR', 'FACILITY_INFO_URL', 'REGIONAL_PROFILE_URL', 'LOCATION_STREET_NO', 'LOCATION_STREET1', 'LOCATION_CITY', 'LOCATION_COUNTY', 'LOCATION_STATE', 'LOCATION_ZIP', 'FACILITY_CONTACT_NAME', 'FACILITY_CONTACT_TEL', 'CONTACT_EMAIL_ADDRESS', 'CONTACT_PHONE_AND_EXT', 'ENTIRE_FACILITY_IND', 'AREA_NAME', 'AREA_NAME_DESCRIPTION', 'CLEARED_PUBLIC_RELEASE', 'GIS_FEATURE_LAST_CHANGE_DATE', 'DATA_COLLECTION_DATE', 'AREANAME_INFO_URL_DESC', 'TIER_ACCURACY_CODE', 'EVENT_SEQ', 'EVENT_CODE', 'ACTUAL_DATE', 'REGIONAL_SUPP_CONTROL_ID', 'HORIZONTAL_ACC_MEASURE', 'HORIZONTAL_COLL_DESC', 'TIER_ACCURACY_DESC', 'FEDERAL_FACILITY', 'CONTROL_URL', 'CONTROL_DESC', 'HORIZONTAL_COLL_CODE','SHAPE@']
    if geoType == 'Point':
        fieldArray.append('SITE_AREA_ACREAGE')
    ## looping through the feature class and populating the event control information
    with arcpy.da.UpdateCursor(in_table=inputCopy, field_names=fieldArray, sql_clause=("ORDER BY", "HANDLER_ID")) as cursor: # , sort_fields='HANDLER_ID A'
        arcpy.AddMessage('Populating feature class with data from the web service...')
        # print('Populating feature class with data from the web service...')
        destCursor = arcpy.da.InsertCursor(outClass, fieldArray)
        # destCursor = arcpy.da.InsertCursor(outClass, field_names)

        # store each HANDLER_ID to check if the current record has already been found in the web service
        selHandlerId = ''

        for row in cursor:
            ## Pass EPA ID to function.
            if row[fieldArray.index('HANDLER_ID')] != selHandlerId:
                selHandlerId = row[fieldArray.index('HANDLER_ID')]
                print('Get data for: ' + str(row[fieldArray.index('HANDLER_ID')]))
                returnJSON = getAttributes(row[fieldArray.index('HANDLER_ID')], badSites)

            ## if the epaID is bad or not in the web service, this part will be skipped.
            if returnJSON:

                try:
                    row[fieldArray.index('REGION')] = returnJSON['REGION']
                except:
                    pass
                try:
                    row[fieldArray.index('HANDLER_NAME')] = returnJSON['HANDLER_NAME']
                except:
                    pass
                try:
                    row[fieldArray.index('LOCATION_STREET1')] = returnJSON['LOCATION_STREET']
                except:
                    pass
                try:
                    row[fieldArray.index('LOCATION_CITY')] = returnJSON['LOCATION_CITY']
                except:
                    pass
                try:
                    row[fieldArray.index('LOCATION_COUNTY')] = returnJSON['LOCATION_COUNTY_NAME']
                except:
                    pass
                try:
                    row[fieldArray.index('LOCATION_STATE')] = returnJSON['LOCATION_STATE']
                except:
                    pass
                try:
                    row[fieldArray.index('LOCATION_ZIP')] = returnJSON['LOCATION_ZIP']
                except:
                    pass
                try:
                    row[fieldArray.index('FACILITY_CONTACT_NAME')] = returnJSON['CONTACT_NAME']
                except:
                    pass
                try:
                    row[fieldArray.index('FACILITY_CONTACT_TEL')] = returnJSON['CONTACT_PHONE']
                except:
                    pass
                try:
                    row[fieldArray.index('CONTACT_EMAIL_ADDRESS')] = returnJSON['CONTACT_EMAIL_ADDRESS']
                except:
                    pass
                try:
                    row[fieldArray.index('CONTACT_PHONE_AND_EXT')] = returnJSON['CONTACT_PHONE']
                except:
                    pass
                # try:
                #     row[21] = returnJSON['LAST_UPDATE_DATE']
                # except:
                #     pass
                if geoType == 'Point':
                    try:
                        row[fieldArray.index('SITE_AREA_ACREAGE')] = returnJSON['AREA_ACREAGE']
                    except:
                        pass
                try:
                    row[fieldArray.index('FED_WASTE_GENERATOR')] = returnJSON['FED_WASTE_GENERATOR_CODE']
                except:
                    pass
                
                ## varible to determine if there are matching controls by AREA_NAME
                matchedInst = False
                matchedEng = False

                ## combine the IC lists into a full list of IC's for the site
                fullICList = returnJSON['IC_EP'] + returnJSON['IC_PR'] + returnJSON['IC_GC'] + returnJSON['IC_ID']

                ## loop through control events and create copies for each IC_EP control type.
                for event in fullICList:
                    # print (event)
                    if row[fieldArray.index('AREA_NAME')] == event['EventArea']:
                        matchedInst = True
                        try:
                            row[fieldArray.index('EVENT_SEQ')] = event['EventSequence']
                        except:
                            pass

                        try:
                            row[fieldArray.index('EVENT_CODE')] = event['EventCode']
                        except:
                            pass

                        try:
                            row[fieldArray.index('ACTUAL_DATE')] = event['EventDate']
                        except:
                            pass

                        try:
                            row[fieldArray.index('ENTIRE_FACILITY_IND')] = event['FacilityWideIndicator']
                        except:
                            pass

                        try:
                            cursor.updateRow(row)
                        except:
                            print('Error: ', row[fieldArray.index('HANDLER_ID')])
                            pass

                        destCursor.insertRow(row)
                        log.write('Site '+str(row[fieldArray.index('HANDLER_ID')])+' is an IC_EP control. \n')

                ## combine the EC lists into a full list
                fullECList = returnJSON['EC_NG'] + returnJSON['EC_GW']

                for event in fullECList:
                    if row[fieldArray.index('AREA_NAME')] == event['EventArea']:
                        matchedEng = True
                        try:
                            row[fieldArray.index('EVENT_SEQ')] = event['EventSequence']
                        except:
                            pass

                        try:
                            row[fieldArray.index('EVENT_CODE')] = event['EventCode']
                        except:
                            pass

                        try:
                            row[fieldArray.index('ACTUAL_DATE')] = event['EventDate']
                        except:
                            pass

                        try:
                            row[fieldArray.index('ENTIRE_FACILITY_IND')] = event['FacilityWideIndicator']
                        except:
                            pass

                        try:
                            cursor.updateRow(row)
                        except:
                            print('Error: ', row[fieldArray.index('HANDLER_ID')])
                            pass

                        destCursor.insertRow(row)
                        log.write('Site '+str(row[fieldArray.index('HANDLER_ID')])+' is an EC_NG control. \n')

                ## if there are no institution or engineering controls, then just push the row as is to the outClass
                if len(fullECList) < 1 and len(fullICList) < 1:
                    destCursor.insertRow(row)
                    log.write('Site '+str(row[fieldArray.index('HANDLER_ID')])+' has no controls. \n')
                ## add the resulting feature even if it doesn't have any matching controls by AREA_NAME
                elif (row[18] is None):
                    destCursor.insertRow(row)
                    log.write('Site '+str(row[fieldArray.index('HANDLER_ID')])+' has no AREA NAME value. \n')
                ## if there is an AREA_NAME present but it doesn't match any of the controls for the site then add the feature to the output
                ## most likely these are extra polygons that regions added to the geospatial data but not to RCRAInfo
                elif (matchedEng == False and matchedInst == False):
                    destCursor.insertRow(row)
                    log.write('Site '+str(row[fieldArray.index('HANDLER_ID')])+' has an AREA NAME value but it does not match controls in the service. \n')
            else:
                print('Site: ' + str(row[fieldArray.index('HANDLER_ID')])+' not found in the RCRAInfo web service. \n')
                log.write('Site '+str(row[fieldArray.index('HANDLER_ID')])+' not found in the RCRAInfo web service. \n')
                ## writing the record to output even though there was nothing retrieved from the web service.
                destCursor.insertRow(row)
        del destCursor

        # arcpy.AddMessage('Data retrieved...')
        # print(('Data retrieved...'))
    
    if geoType == 'Polygon':
        ## calculating area for polygons
        arcpy.CalculateGeometryAttributes_management(outClass, "POLY_AREA_ACREAGE AREA_GEODESIC", '', "ACRES", None, "SAME_AS_INPUT")
        arcpy.AddMessage('Calculating polygon area...')

    ## message displayed on successful execution. Log file is closed
    arcpy.FeatureClassToFeatureClass_conversion(outClass, outClassLocation, outputName)
    arcpy.AddMessage('Output feature class successfully created!')
    # print('Output feature class successfully created!')
log.write('\n')
log.write('\n')
log.write('There were '+str(len(badSites))+' EPA IDs that were not successful: \n')
for site in badSites:
    log.write(site+'\n')
log.close()