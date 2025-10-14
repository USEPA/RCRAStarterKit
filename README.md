This is an ArcGIS Pro toolbox that helps align RCRA data to the EPA RCRA data model.
It has 4 tools:
 1 - Update to RCRA Schema - This takes the input data and converts it to the EPA RCRA data model schema. It outputs a new data layer
 2 - Get Event Data - Generates .csv files that lists the associated events from RCRAInfo to the input dataset. The csv will be saved to the same location as the input layer
 3 - Get Evaluation Data - Generates .csv files that lists the associated evaluation from RCRAInfo to the input dataset. The csv will be saved to the same location as the input layer
 4 - Generate GeoJSON - Checks that the required fields are populated and creates a GeoJSON file. At the current time this does not include the Event or Evaluation data.
