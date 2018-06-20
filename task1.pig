REGISTER '/home/acadgild/install/pig/pig-0.16.0/lib/piggybank.jar';
crime_data = LOAD 'Crimes_-_2001_to_present.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',' ) as (ID: chararray, Case_Num: chararray, date: chararray, block: chararray, IUCR: chararray, type: chararray, description: chararray, arrest:chararray, domestic :chararray, beat:chararray, district:chararray, ward:chararray, area:chararray, FBI_Code:chararray, X:chararray, Y:chararray, year: int , updated_on : chararray, lat: chararray, longitude: chararray, location:chararray);
filter_crime_data = FILTER crime_data BY (ID IS NOT NULL) AND (Case_Num IS NOT NULL) AND (date IS NOT NULL) AND (block IS NOT NULL) AND (IUCR IS NOT NULL) AND (type IS NOT NULL) AND (description IS NOT NULL) AND (arrest IS NOT NULL) AND (domestic IS NOT NULL) AND (beat IS NOT NULL) AND (district IS NOT NULL) AND (ward IS NOT NULL) AND (area IS NOT NULL) AND (FBI_Code IS NOT NULL) AND (X IS NOT NULL) AND (Y IS NOT NULL) AND (year IS NOT NULL) AND (updated_on IS NOT NULL) AND (lat IS NOT NULL) AND (longitude IS NOT NULL) AND (location IS NOT NULL); 
grouped_data =  GROUP filter_crime_data BY FBI_Code;
noCases_fbiCode = FOREACH grouped_data GENERATE FLATTEN(group) AS FBI_Code , COUNT(filter_crime_data.FBI_Code);
STORE noCases_fbiCode INTO 'TASK1Output' USING PigStorage(',');
dump noCases_fbiCode;
