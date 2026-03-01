
########################################################################
#Loading in data from Open Payments website. Only selecting necessary columns
########################################################################

#creating open payments 2019-2024 table
CREATE TABLE op (number double,
Covered_Recipient_NPI VARCHAR(20),
Recipient_City VARCHAR(100),
Recipient_State VARCHAR(100),
Recipient_Zip_Code double, 
Total_Amount_of_Payment_USDollars double,
Date_of_Payment VARCHAR(100),
Form_of_Payment_or_Transfer_of_Value VARCHAR(100),
Nature_of_Payment_or_Transfer_of_Value VARCHAR(100),
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 VARCHAR(100),
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 VARCHAR(100),
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 VARCHAR(100),
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 VARCHAR(100),
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 VARCHAR(100));

#DROP TABLE op;

SET GLOBAL local_infile = 'ON';

LOAD DATA LOCAL INFILE 'Open_Payments_Data/test/OP_19-24_bloom.csv'
INTO TABLE OP
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM op;


##################################################################################
#Getting tables for Tableau
##################################################################################
##################################################################################
#Table 1: Trends of Payment Encounters Per Year Quarter
	#The Open Payments dataset does not come with a specific quarter column, but
    #it does come with a Date_of_Payment column which I will transform in to a 
    #year quarter column and get the payment quarter trends
  
#Making the Date_of_Payment column a date 
SET SQL_SAFE_UPDATES = 0;

UPDATE op
SET Date_of_Payment = STR_TO_DATE(Date_of_Payment, '%m/%d/%Y');

ALTER TABLE op
MODIFY COLUMN Date_of_Payment DATE;


#Now creating a year_quarter column 
ALTER TABLE op ADD COLUMN year_quarter VARCHAR(7),
ADD COLUMN year varchar(4), 
ADD COLUMN quarter varchar(4);

UPDATE op
SET 
    year = YEAR(Date_of_Payment),
    quarter = QUARTER(Date_of_Payment),
    year_quarter = CONCAT(YEAR(Date_of_Payment), '-', QUARTER(Date_of_Payment));


#Now I will create a count of encounters per year quarter in Bloomington, IN
SELECT COUNT(*) AS payment_encounters, 
year_quarter
FROM op 
GROUP BY year_quarter
ORDER BY year_quarter;

##################################################################################
#Table 2: Payment Types with the most payment encounters total and each year

#percent of total payments that are each type
SELECT Nature_of_Payment_or_Transfer_of_Value AS Payment_Type,
    COUNT(*) AS COUNT,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percent
FROM op
GROUP BY Payment_Type
ORDER BY COUNT;


##################################################################################
#Table 3: Payment Types with the most payment encounters by each year_quarter

#percent of total payments that are each type
SELECT Nature_of_Payment_or_Transfer_of_Value AS Payment_Type,
    COUNT(*) AS COUNT, 
    year_quarter
FROM op
GROUP BY Payment_Type, year_quarter
ORDER BY year_quarter;


##################################################################################
#Table 4: Payment Types with the most payment encounters total and each year

#Sum of payments for each payment type
SELECT Nature_of_Payment_or_Transfer_of_Value AS Payment_Type,
    SUM(Total_Amount_of_Payment_USDollars) AS Payment
FROM op
GROUP BY Payment_Type
ORDER BY Payment;


##################################################################################
#Table 5: Drug/Device/Supply with highest number of encounters total and each year 
	#For this dataset there are 5 columns that could have a device/drug/supply name.
    #So in order to approporiately cout the amounts I am going to count each device/drug/supply name column 
    #individually and then combine their counts. 

#Cleaning the data first
UPDATE op
SET Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 = UPPER(Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1),
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 = UPPER(Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2),
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 = UPPER(Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3),
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 = UPPER(Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4),
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 = UPPER(Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5);

#selecting the columns with drugnames and combining them in to a single column count 
#to get total number of payment encounters per drug
SELECT Drug_Name AS Drug_Names,
COUNT(*) AS COUNT
FROM (
        
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 AS Drug_Name 
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 IS NOT NULL 
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 != ''
        UNION ALL
        
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 AS Drug_Name
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 IS NOT NULL 
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 != ''
        UNION ALL
       
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 AS Drug_Name
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 IS NOT NULL 
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 != ''
        UNION ALL
        
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 AS Drug_Name
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 IS NOT NULL 
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 != ''
        UNION ALL
        
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 AS Drug_Name
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 IS NOT NULL 
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 != ''
        
    ) AS combined_data
GROUP BY
    Drug_Names
ORDER BY
    COUNT;


##################################################################################
#Table 6: Vraylar and SkyRizi Trends year quarter
	#Since Vraylar and SkyRizi are the two drugs with the most payment encounters,
	#Lets look at the spending trends between Skyrizi and Vraylar over the years
    
SELECT Drug_Name AS Drug_Names,
event_year AS year_quarter,
SUM(op_spend) AS op_spend
FROM (
        
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 AS Drug_Name, year_quarter AS event_year,
        Total_Amount_of_Payment_USDollars AS op_spend
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 IS NOT NULL 
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 != ''
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 LIKE '%SKYRIZI%' 
        OR Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 LIKE '%VRAYLAR%'
        UNION ALL
        
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 AS Drug_Name, year_quarter AS event_year,
        Total_Amount_of_Payment_USDollars AS op_spend
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 IS NOT NULL 
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 != ''
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 LIKE '%SKYRIZI%'
        OR Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 LIKE '%VRAYLAR%'
        UNION ALL
       
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 AS Drug_Name, year_quarter AS event_year,
        Total_Amount_of_Payment_USDollars AS op_spend
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 IS NOT NULL 
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 != ''
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 LIKE '%SKYRIZI%'
        OR Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 LIKE '%VRAYLAR%'
        UNION ALL
        
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 AS Drug_Name, year_quarter AS event_year,
        Total_Amount_of_Payment_USDollars AS op_spend
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 IS NOT NULL 
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 != ''
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 LIKE '%SKYRIZI%'
        OR Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 LIKE '%VRAYLAR%'
        UNION ALL
        
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 AS Drug_Name, year_quarter AS event_year,
        Total_Amount_of_Payment_USDollars AS op_spend
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 IS NOT NULL 
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 != ''
        AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 LIKE '%SKYRIZI%'
        OR Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 LIKE '%VRAYLAR%'
        
    ) AS combined_data
GROUP BY
    Drug_Names,
    year_quarter
ORDER BY
    year_quarter;
    

##################################################################################
#Table 7: SKYRIZI and VRAYLAR ad spend trends 2019-2024. 

#creating Skyrizi ad spend 2019-2024 table
CREATE TABLE skyrizi_ad (year_quarter VARCHAR(100),
ad_spend VARCHAR(100)); 
 
LOAD DATA LOCAL INFILE "C:/Users/madkpott/Downloads/SKYRIZI_ADSPEND.csv"
INTO TABLE skyrizi_ad
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT * FROM skyrizi_ad;

UPDATE skyrizi_ad
SET year_quarter = CONCAT(SUBSTRING_INDEX(year_quarter, ' ', -3), '-', 
SUBSTRING_INDEX(SUBSTRING_INDEX(year_quarter, ' ', 2), ' ', -1));

UPDATE skyrizi_ad
SET year_quarter = REPLACE(year_quarter, ' ', '');

ALTER TABLE skyrizi_ad 
ADD drug VARCHAR(100) NOT NULL DEFAULT 'SKYRIZI';


#creating vraylar ad spend 2019-2024 table
CREATE TABLE vraylar_ad (year_quarter VARCHAR(100),
ad_spend VARCHAR(100));

LOAD DATA LOCAL INFILE "C:/Users/madkpott/Downloads/VRAYLAR_ADSPEND.csv"
INTO TABLE vraylar_ad
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT * FROM vraylar_ad;

UPDATE vraylar_ad
SET year_quarter = CONCAT(SUBSTRING_INDEX(year_quarter, ' ', -3), '-', 
SUBSTRING_INDEX(SUBSTRING_INDEX(year_quarter, ' ', 2), ' ', -1));

UPDATE vraylar_ad
SET year_quarter = REPLACE(year_quarter, ' ', '');

ALTER TABLE vraylar_ad 
ADD drug VARCHAR(100) NOT NULL DEFAULT 'VRAYLAR';

SELECT *
FROM skyrizi_ad
UNION ALL
SELECT *
FROM vraylar_ad
ORDER BY year_quarter;


##############################################################
#table 8 Skyrizi and Vraylar ad and open payment spending
	#creating a different spending view that combines skyrizi and 
	#vraylar opn payment and ad spending in to one view

WITH open_payment AS (
    SELECT Drug_Name AS drug,
           event_year AS year_quarter,
           SUM(op_spend) AS op_spend
    FROM (
        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 AS Drug_Name, 
               year_quarter AS event_year,
               Total_Amount_of_Payment_USDollars AS op_spend
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 IS NOT NULL 
          AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 != ''
          AND (Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 LIKE '%SKYRIZI%' 
           OR  Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 LIKE '%VRAYLAR%')
        UNION ALL

        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2, 
               year_quarter,
               Total_Amount_of_Payment_USDollars
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 IS NOT NULL 
          AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 != ''
          AND (Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 LIKE '%SKYRIZI%' 
           OR  Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 LIKE '%VRAYLAR%')
        UNION ALL

        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3, 
               year_quarter,
               Total_Amount_of_Payment_USDollars
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 IS NOT NULL 
          AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 != ''
          AND (Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 LIKE '%SKYRIZI%' 
           OR  Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 LIKE '%VRAYLAR%')
        UNION ALL

        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4, 
               year_quarter,
               Total_Amount_of_Payment_USDollars
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 IS NOT NULL 
          AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 != ''
          AND (Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 LIKE '%SKYRIZI%' 
           OR  Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 LIKE '%VRAYLAR%')
        UNION ALL

        SELECT Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5, 
               year_quarter,
               Total_Amount_of_Payment_USDollars
        FROM op
        WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 IS NOT NULL 
          AND Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 != ''
          AND (Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 LIKE '%SKYRIZI%' 
           OR  Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 LIKE '%VRAYLAR%')

    ) AS combined_data
    GROUP BY drug, year_quarter
),

ad_spend AS (
    SELECT * FROM skyrizi_ad
    UNION ALL
    SELECT * FROM vraylar_ad
    ORDER BY year_quarter
)

SELECT
    year_quarter,
    drug,
    op_spend,
    ad_spend
FROM open_payment
JOIN ad_spend USING (year_quarter, drug);







