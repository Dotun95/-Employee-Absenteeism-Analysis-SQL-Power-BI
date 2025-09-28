# Data cleaning
# create staging table for absentee dataset

CREATE TABLE absent
LIKE
	absenteeism_at_work;
    
SELECT *
FROM
	absent;

INSERT 
		absent
SELECT *
FROM
		absenteeism_at_work;

#Now we can carry on using the absent dataset

SELECT *
FROM
	absent;
    
# Check for duplicates

WITH duplicate_cte AS
(
SELECT *,
		ROW_NUMBER() OVER (PARTITION BY ID, Reason_for_absence, Month_of_absence, Day_of_the_week, Seasons, Transportation_expense, DistancefromResidencEtoWork, Servicetime, Age, Workloadday, Hittarget, Disciplinarfailure, Education, Son, Socialdrinker, Socialsmoker, Pet, Weight, Height, Bodymassindex, Absenteeismtiminhours) AS row_num
FROM absent)
SELECT *
FROM
	duplicate_cte
WHERE
	row_num >1;
    
#No duplicate in the dataset, next check for unnecessary columns and delete them

ALTER TABLE
			absent
DROP COLUMN 
			seasons;
            
ALTER TABLE
			absent
DROP COLUMN
			servicetime,
DROP COLUMN
            disciplinarfailure,
DROP COLUMN
			education,
DROP COLUMN          
            son,
DROP COLUMN	
			socialdrinker,
DROP COLUMN            
			socialsmoker,
DROP COLUMN 
           pet,
DROP COLUMN 
           weight,
DROP COLUMN 
            height;
            
SELECT *
FROM
	absent;

/* Table is standardized and no null/ blank values*/

/* Join the absent table and reason table to see the reason for absence
also join the compensation table*/

SELECT *
FROM 	
	absent t1
LEFT JOIN
		reasons t2
ON
	t1.Reason_for_absence = t2.Number
JOIN 
	 compensation t3
ON 
	t1.ID = t3.ID;
    
#Create this display as a view

CREATE VIEW
			absent_view
AS
	SELECT 	
			t1.ID as absent_id,
            t1.Reason_for_absence, 
            t1.Month_of_absence, 
            t1.Day_of_the_week, 
            t1.Transportation_expense, 
            t1.Distance, 
            t1.Age, 
            t1.Workload, 
            t1.Hit_target,
            t1.bmi, 
            t1.Absentee_hours, 
            t2.Number, 
            t2.Reason, 
            t3.ID as compensation_id,
            t3.`comp/hr`
FROM 	
	absent t1
 LEFT JOIN
		reasons t2
ON
	t1.Reason_for_absence = t2.Number
JOIN 
	 compensation t3
ON 
	t1.ID = t3.ID;
    
# Display view
SELECT *
FROM
	absent_view;

#Now that we have all the information in the absent_view we can continue with the analysis

#1. Total absentee hours per reason

SELECT 
		reason,
        SUM(Absentee_hours) AS total_absenthours
FROM
		absent_view
GROUP BY
		reason
ORDER BY 
		total_absenthours DESC;
        
#2.	Top Reasons for Absence

SELECT 
		reason,
        count(reason)
FROM  
		absent_view
GROUP BY 
		reason
ORDER BY
		count(reason) DESC;
        


# Relationship between distance to work and hours absent
/* check the highest and least distance in order to properly construct the case statement*/    
SELECT
		MAX(distance), MIN(distance)
FROM
		absent_view;

SELECT
		(CASE
			WHEN distance <20 THEN 'less than 20 miles'
			WHEN distance BETWEEN 20 AND 40 THEN '20-40 miles'
			WHEN distance >40 THEN 'more than 40 miles'
			ELSE 'other' END) AS distance_range,
	    AVG(absentee_hours) AS avg_absent_hrs
FROM
	absent_view
GROUP BY	
		distance_range
ORDER BY
		avg_absent_hrs DESC;


            
# Relationship between compensation per hr and absent hours
/* check the highest and least compesation value in order to properly construct the case statement*/

SELECT
		MAX(`comp/hr`), MIN(`comp/hr`)
FROM
		absent_view;

SELECT
		(CASE
			WHEN `comp/hr` < 30 THEN 'less than $30'
			WHEN `comp/hr` BETWEEN 30 AND 40 THEN '$30-$40'
			WHEN `comp/hr` > 40 THEN 'more than $40'
			ELSE 'other' END) AS compensation_range,
		AVG(absentee_hours) AS average_absent_hrs
FROM 
		absent_view
GROUP BY 
		compensation_range
ORDER BY
		 average_absent_hrs;


# Relationship between transportation expenses and absent hours
/* check the highest and least transportation expense in order to properly construct the case statement*/

SELECT
		MAX(transportation_expense), MIN(transportation_expense)
FROM
		absent_view;

SELECT 
		(CASE
			WHEN transportation_expense < 200 THEN 'less than $200'
			WHEN transportation_expense BETWEEN 200 AND 300 THEN '$200-$300'
			WHEN transportation_expense > 300 THEN 'more than $300'
			ELSE 'other' END) AS transportation_range,
		AVG(absentee_hours) AS avg_absent_hrs
FROM
		absent_view
GROUP BY
		transportation_range
ORDER BY 
		avg_absent_hrs;

# Relationship between body mass index(bmi) and absent hours
/* check the highest and least bmi value in order to properly construct the case statement*/

SELECT 
		MAX(bmi),
        MIN(bmi)
FROM
		absent_view;
        
SELECT
		(CASE
			WHEN bmi < 18.5 THEN 'underweight'
            WHEN bmi BETWEEN 18.5 AND 24.9 THEN 'healthy'
            WHEN bmi BETWEEN 24.9 AND 29.9 THEN 'overweight'
            WHEN bmi > 29.9 THEN 'obesity'
            ELSE 'other' END) AS bmi_range,
		AVG(absentee_hours) AS avg_absent_hrs
FROM 
		absent_view
GROUP BY
		bmi_range
ORDER BY
		avg_absent_hrs DESC;

#Relationship between age and absent hours
/* check the highest and least age in order to properly construct the case statement*/

SELECT
		MAX(age),
        MIN(age)
FROM
		absent_view;
        
SELECT 
		(CASE 
			WHEN age < 33 THEN 'young adult'
            WHEN age BETWEEN 33 AND 48 THEN 'middle age'
            WHEN age > 48 THEN 'old'
            ELSE 'other' END) AS age_group,
		AVG(absentee_hours) AS avg_absent_hrs
FROM
		absent_view
GROUP BY 
		age_group
ORDER BY 
        avg_absent_hrs DESC;
        
#Relationship workload and absent hours
/* check the highest and least workload in order to properly construct the case statement*/

SELECT
		MAX(workload), 
        MIN(workload)
FROM
		absent_view;
        
SELECT 
		(CASE
				WHEN workload < '250000' THEN 'small workload'
                WHEN workload BETWEEN '250000' AND '350000' THEN 'medium workload'
                WHEN workload > '350000' THEN 'large workload'
                ELSE 'other' END) AS workload_category,
		AVG(absentee_hours) as avg_absent_hrs
FROM
		absent_view
GROUP BY
		workload_category
ORDER BY
		avg_absent_hrs DESC;
 
 #Relationship target and absent hours
/* check the highest and least target value in order to properly construct the case statement*/
SELECT 
		MAX(hit_target),
        MIN(hit_target)
FROM 
		absent_view;
        
SELECT 
		(CASE
				WHEN hit_target <85 THEN 'slacking'
                WHEN hit_target BETWEEN 85 AND 95 THEN 'moderate'
                WHEN hit_target > 95 THEN 'effecient'
                ELSE 'other' END) AS target_group,
		AVG(absentee_hours) AS avg_absent_hrs
FROM
		absent_view
GROUP BY
		target_group
ORDER BY
		avg_absent_hrs DESC;
 #Relationship between days of the week and absent hours       
SELECT 	
		DAY_OF_THE_WEEK,
        AVG(absentee_hours) AS avg_absent_hrs
FROM
		absent_view
GROUP BY
		DAY_OF_THE_WEEK
ORDER BY
		avg_absent_hrs DESC;
   
#Relationship between absent hours and month
SELECT 
		month_of_absence,
        AVG(absentee_hours) AS avg_absent_hrs
FROM 
		absent_view
GROUP BY 
		month_of_absence
ORDER BY 
		avg_absent_hrs DESC;

