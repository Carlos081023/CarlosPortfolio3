-- Data Cleaning Project

-- Viewing Table
SELECT *
FROM layoffs
LIMIT 10;

SELECT COUNT(*)
FROM layoffs;

DESCRIBE layoffs;
-- The dataset has nine columns with 2361 records. 
-- Now that I have an idea of the table I will begin the data cleaning process

-- Let's begin by finding any duplicate entries within the dataset
WITH duplicate_data AS(
SELECT
	company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    `date`,
    stage,
    country,
    funds_raised_millions,
    ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as duplicates
FROM layoffs)
SELECT *
FROM duplicate_data
WHERE duplicates > 1;
-- This query will use the ROW_NUMBER to find any and all duplicate entries by labeling them as 2. I created a CTE and then queried to show the duplicate entries only.

-- Now I want to remove these specific entries.
WITH duplicate_data AS(
SELECT
	company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    `date`,
    stage,
    country,
    funds_raised_millions,
    ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as duplicates
FROM layoffs),
duplicates_only AS(
SELECT *
FROM duplicate_data
WHERE duplicates > 1)
DELETE l
FROM layoffs as l
JOIN duplicates_only as d
	ON l.company=d.company;

-- I decided to use a DELETE FROM statement to remove the duplicate entries. The CTE used a window function, row_number(), to assign a number to each row however any
-- duplicate entry will get a '2' signifying that it is a duplicate entry. Then I ran a query on the cte to return all records where the duplicates are shown
-- From there I decided instead of creating a column into the original table, I would try to join the CTEs with the original table and DELETE all records where the
-- duplicate entries were found in the CTEs.

-- Now that all duplicate records are removed, let's go ahead with fixing all structural errors with the table. It was originally imported with very lax rules
-- otherwise it would not be properly imported into the RDBMS. 

-- Company, Location, Industry column is a text column to which I am okay with so I will leave it as is
-- total_laid_off is an INT column which is appropiate since it represents the number of people who were laid off. Can't have decimals

-- Viewing column
SELECT percentage_laid_off
FROM layoffs;
-- The column is set as a text data type but it is not correct due to the numeric values within the column, I will convert this now
-- Viewing the range of values within the column
SELECT 	
	CONCAT(MIN(percentage_laid_off),' ',MAX(percentage_laid_off)) as data_range
FROM layoffs;
-- The values are stored as 0 to 1 with anything between as a decimal. I need to handle the int values first
SELECT 
	percentage_laid_off,
	CAST(percentage_laid_off AS DECIMAL(3,2))
FROM layoffs
WHERE  percentage_laid_off IN (0,1);
-- This is a check of my data conversion and making sure it applied to the integer numbers. Now that I am sure of my change I will update the column values
UPDATE layoffs
SET percentage_laid_off = CAST(percentage_laid_off AS DECIMAL(3,2));

ALTER TABLE layoffs
MODIFY COLUMN percentage_laid_off DECIMAL(3,2);

-- Viewing column
SELECT `date` 
FROM layoffs;
-- Dates are stored in a MM/DD/YYYY format, I will convert this to YYYY-MM-DD format for MySQL but first I will change the column name to something else
ALTER TABLE layoffs
RENAME COLUMN `date` TO layoff_date;

SELECT 
	layoff_date,
    STR_TO_DATE(layoff_date, '%m/%d/%Y') AS string_date,
    DATE(STR_TO_DATE(layoff_date, '%m/%d/%Y')) AS formatted_date
FROM layoffs;

-- After a test check of my conversion, I will now implement the changes.
UPDATE layoffs
SET layoff_date =  DATE(STR_TO_DATE(layoff_date, '%m/%d/%Y'));

ALTER TABLE layoffs
MODIFY COLUMN layoff_date DATE;

-- All other columns look appropiate in terms of the data structures, now I will go and identify any missing or NULL values

SELECT
	company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    layoff_date,
    stage,
    country,
    funds_raised_millions
FROM layoffs
WHERE company IS NULL
	OR location IS NULL
    OR industry IS NULL
    OR layoff_date IS NULL
    OR stage IS NULL
    OR country IS NULL;

-- Columns with NULL values are the total_laid_off, percentage_laid_off, and funds_raised_millions. I cannot populate the missing values so these will be left as NULL however I will remove records where both total_laid_off and percent_ are both NULL

-- These records have incomplete information and cannot be populated. I will remove these nulls
SELECT *
FROM layoffs
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE FROM layoffs
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT 
	funds_raised_millions,
    COALESCE(funds_raised_millions, 0)
FROM layoffs
WHERE funds_raised_millions IS NULL;

SELECT stage
FROM layoffs
GROUP BY stage;
SELECT *
FROM layoffs
WHERE company LIKE 'Bally%';

-- Checking for any empty strings or missing string values

SELECT
	company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    layoff_date,
    stage,
    country,
    funds_raised_millions
FROM layoffs
WHERE company = ''
	OR location = ''
    OR industry = ''
    OR stage = ''
    OR country = '';
    
-- In terms of missing values in string columns, there are 3 values. I will try to populate them if possible 
SELECT
	company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    layoff_date,
    stage,
    country,
    funds_raised_millions
FROM layoffs
WHERE company IN ('Airbnb','Juul','Carvana');

-- Using the company name, I was able to find the industry through other records. I will now update the missing values
UPDATE layoffs
SET industry = 'Transportation'
WHERE company = 'Carvana';
UPDATE layoffs
SET industry = 'Consumer'
WHERE company = 'Juul';
UPDATE layoffs
SET industry = 'Travel'
WHERE company = 'Airbnb';
UPDATE layoffs
SET industry = 'Gambling Facilities and Casinos'
WHERE company LIKE 'Bally%';

-- All empty or missing strings are dealt with

-- Checking for inconsistent labels and field lengths in certain columns

SELECT 
	company,
	length(company),
	LENGTH(TRIM(company))
FROM layoffs
WHERE length(company) <> LENGTH(TRIM(company))
GROUP BY company;
-- Company had trailing spaces, removing them and update the table
SELECT 
	company,
    TRIM(company)
FROM layoffs;
UPDATE layoffs
SET company = TRIM(company);

SELECT 
	location,
    LENGTH(location) as len,
	LENGTH(TRIM(location))
FROM layoffs
WHERE length(location) <>  LENGTH(TRIM(location))
GROUP BY location
ORDER BY location DESC;
-- 'DÃ¼sseldorf', '13'
-- 'Dusseldorf', '10'
-- 'MalmÃ¶', '8'
-- 'FlorianÃ³polis', '16'

-- I found some strange location name, I will find the appropiate name replace them
UPDATE layoffs
SET location = 'Malmo'
WHERE location = 'MalmÃ¶';
UPDATE layoffs
SET location = 'Dusseldorf'
WHERE location = 'DÃ¼sseldorf';
UPDATE layoffs
SET location = 'Florianopolis'
WHERE location = 'FlorianÃ³polis';

-- Identified an entry that says 'Non US'. I will investigate further to understand
SELECT *
FROM layoffs
WHERE location LIKE 'Non%';
-- Given the company's country is available, I will go ahead and insert the location if public information is available.
UPDATE layoffs
SET location = 'Zhejiang'
WHERE company = 'WeDoctor';
UPDATE layoffs
SET location = 'Seychelles'
WHERE company = 'BitMEX';
UPDATE layoffs
SET country = 'Africa'
WHERE company = 'BitMEX';
SELECT *
FROM layoffs
WHERE country = 'Africa';
-- A few companies had issues with their location. I was able to update the location and even countries for some companies.

-- Viewing industry column
SELECT 
	industry,
    LENGTH(industry) as len,
	LENGTH(TRIM(industry))
FROM layoffs
WHERE LENGTH(industry) <> LENGTH(TRIM(industry))
GROUP BY industry
ORDER BY industry DESC;

SELECT *
FROM layoffs
WHERE industry IS NULL;
-- A few issues with this column are evident. A NULL value for a company's industry and a few labels are not coherent. Crypto industry has been repeated three times
-- just in different ways of writing it. I will update now.

-- I'll start off by cleaning the mislabeled crypto industry
SELECT
	industry,
    COUNT(industry)
FROM layoffs
WHERE industry LIKE 'Crypto%'
GROUP BY industry;

SELECT 
	industry,
    CASE 
		WHEN industry LIKE 'Crypto%' THEN 'Crypto'
	END as crypto_fix
FROM layoffs
WHERE industry LIKE 'Crypto%'
GROUP BY industry;
-- Clean was successful, i will update now.
UPDATE layoffs
SET industry = 'Crypto'
WHERE  industry LIKE 'Crypto%';

UPDATE layoffs
SET industry = 'Human Resources'
WHERE  industry LIKE 'HR';

-- Checking Stage
SELECT 
	stage,
    LENGTH(stage) as len,
    LENGTH(TRIM(stage))
FROM layoffs
GROUP BY stage
ORDER BY stage DESC;

-- Checking country
SELECT 
	country,
    LENGTH(country) as len,
    LENGTH(TRIM(country)) as trimmed
FROM layoffs
GROUP BY country
ORDER BY country DESC;

-- United States is repeated twice so I will fix the error now
UPDATE layoffs
SET country = 'United States'
WHERE country = 'United States%';

-- Data Validation

-- Throughout the cleaning process I had conducted data validation and I will confirm the data is accurate and correct. 

-- Checking percentage_laid_off nulls
SELECT COUNT(percentage_laid_off)
FROM layoffs;
-- Total number of records is 1567
SELECT 
	COUNT(IFNULL(percentage_laid_off, 1))
FROM layoffs;
-- Total number of records of dataset is 1990
-- What does the difference tell us? There is 423 records that are nulls within the dataset. Let's look at those now

SELECT *
FROM layoffs
WHERE percentage_laid_off IS NULL;

-- While we could keep the records, for the sake of a complete and correct analysis, any records that are nulls will be removed. Luckily the number of NULLS is less than 25% of the records so it won't hurt as much.

CREATE TEMPORARY TABLE layoffs_temp AS
SELECT * 
FROM layoffs;

SELECT *
FROM layoffs_temp;

SELECT COUNT(IFNULL(total_laid_off,1))
FROM layoffs_temp;
SELECT COUNT(total_laid_off)
FROM layoffs_temp;
SELECT 1612/1990;
SELECT COUNT(IFNULL(funds_raised_millions,1))
FROM layoffs_temp;
SELECT COUNT(funds_raised_millions)
FROM layoffs_temp;
SELECT COUNT(IFNULL(funds_raised_millions,1))
FROM layoffs_temp;
SELECT COUNT(*)
FROM layoffs_temp
WHERE (NOT percentage_laid_off IS NULL) AND total_laid_off IS NULL;
SELECT COUNT(*)
FROM layoffs_temp
WHERE (NOT total_laid_off IS NULL) AND percentage_laid_off IS NULL;

DELETE FROM layoffs_temp
WHERE total_laid_off IS NULL;



