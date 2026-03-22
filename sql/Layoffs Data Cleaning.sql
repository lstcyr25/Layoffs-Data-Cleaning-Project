/* =========================================================
   Data Cleaning Project - Layoffs Dataset
   =========================================================
   Objective:
   Clean and standardize layoffs data for accurate analysis.

   Cleaning Steps:
   1. Remove duplicate records
   2. Standardize text fields
   3. Normalize NULL and blank values
   4. Remove unusable rows and helper columns
   =========================================================
*/

-- View raw data
SELECT *
FROM dbo.layoffs;


-- =========================================================
-- 1. Remove Duplicates
-- =========================================================


-- Inspect staging table
Select *
From dbo.layoffs_staging;


-- Assign row numbers to identify duplicate records
-- Rows with row_num > 1 are duplicates
Select company, industry, total_laid_off, [date],
	ROW_NUMBER() OVER (
	Partition By company,[location],industry, total_laid_off, percentage_laid_off, [date], stage
	, country, funds_raised_millions
	ORDER By (SELECT NULL)
	) AS row_num
	FROM dbo.layoffs_staging;


-- Identify duplicate rows using a CTE
WITH Duplicates AS (
	Select company, industry, total_laid_off, [date],
		ROW_NUMBER() OVER (
		Partition By company,[location],industry, total_laid_off, percentage_laid_off, [date], stage
		, country, funds_raised_millions
		ORDER By (SELECT NULL)
		) AS row_num
		FROM dbo.layoffs_staging
)
Select *
From Duplicates
Where row_num > 1;


-- Delete duplicate rows while keeping the first occurrence
WITH Duplicates AS (
	Select company, industry, total_laid_off, [date],
		ROW_NUMBER() OVER (
		Partition By company,[location],industry, total_laid_off, percentage_laid_off, [date], stage
		, country, funds_raised_millions
		ORDER By (SELECT NULL)
		) AS row_num
		FROM dbo.layoffs_staging
)
Delete From Duplicates
Where row_num > 1;


-- =========================================================
-- Create Clean Staging Table (Version 2)
-- =========================================================

-- Create a new staging table with a helper column
Create Table dbo.layoffs_staging2(
	company VARCHAR(MAX),
	[location] VARCHAR(MAX),
	industry VARCHAR(MAX),
	total_laid_off INT,
	percentage_laid_off VARCHAR(MAX),
	[date] DATE,
	stage VARCHAR(MAX),
	country VARCHAR(MAX),
	funds_raised_millions INT,
	row_num INT
);


-- Insert deduplicated data into staging2
INSERT Into dbo.layoffs_staging2
Select company,[location], industry, total_laid_off, percentage_laid_off
, [date], stage, country, funds_raised_millions,
	ROW_NUMBER() OVER (
	Partition By company,[location],industry, total_laid_off, percentage_laid_off, [date], stage
	, country, funds_raised_millions
	ORDER By (SELECT NULL)
	) AS row_num
	FROM dbo.layoffs_staging;


-- Remove remaining duplicate rows
DELETE
FROM dbo.layoffs_staging2
Where row_num > 1;


-- =========================================================
-- 2. Standardize Data
-- =========================================================

-- Trim extra spaces from company names
Select company, Trim(company)
FROM dbo.layoffs_staging2;

Update dbo.layoffs_staging2
Set company = Trim(company);


-- Review distinct industries
Select Distinct industry
FROM dbo.layoffs_staging2;


-- Standardize crypto-related industry labels
Select *
FROM dbo.layoffs_staging2
Where industry Like 'Crypto%';


Update layoffs_staging2
Set industry = 'Crypto'
Where industry Like 'Crypto%';


-- =========================================================
-- Standardize Country Names
-- =========================================================

-- Identify inconsistent United States values
Select Distinct country
FROM dbo.layoffs_staging2
Where country like 'United States%'
Order By 1;


-- Preview country cleanup
Select Distinct country, RTrim(Replace(country, '.', '')) As cleaned_country
FROM dbo.layoffs_staging2
Order By 1;


-- Remove trailing periods from country names
Update dbo.layoffs_staging2
Set country = RTrim(Replace(country, '.', ''))
Where country like 'United States%';


-- =========================================================
-- Standardize Date Column
-- =========================================================

-- Preview date conversion
Select [date] as original_date,
Try_Convert(date, [date], 101) As converted_date 
FROM dbo.layoffs_staging2;

-- Convert string dates to DATE type
Update dbo.layoffs_staging2
Set [date] = Try_Convert(date, [date], 101)

-- Enforce DATE data type
Alter Table dbo.layoffs_staging2
Alter Column [date] Date;


-- =========================================================
-- 3. Handle Null and Blank Values
-- =========================================================

-- Identify invalid percentage_laid_off values
Select *
From dbo.layoffs_staging2
Where total_laid_off is Null
AND percentage_laid_off = 'Null';

-- Normalize inconsistent NULL representations
UPDATE dbo.layoffs_staging2
SET percentage_laid_off = NULL
WHERE LTRIM(RTRIM(percentage_laid_off)) = ''
   OR UPPER(percentage_laid_off) = 'NULL';

-- Inconsistent NULL values (NULL vs 'NULL' vs blanks)
-- were normalized for accurate filtering and analysis.


-- Identify missing industry values
Select *
From dbo.layoffs_staging2
Where industry is Null
OR industry = ''
OR industry ='Null';


-- =========================================================
-- Backfill Industry Using Self-Join
-- =========================================================

-- Preview industry backfill logic
Select t1.industry, t2.industry
From dbo.layoffs_staging2 t1
JOIN dbo.layoffs_staging2 t2
	ON t1.company = t2.company
	AND t1.location = t2.location
WHERE (t1.industry is Null OR t1.industry = '' OR t1.industry = 'NULL')
AND t2.industry is not Null;

-- Update missing industries from valid records
UPDATE t1
SET t1.industry = t2.industry
FROM dbo.layoffs_staging2 t1
JOIN dbo.layoffs_staging2 t2
    ON t1.company = t2.company
WHERE (t1.industry IS NULL
       OR LTRIM(RTRIM(t1.industry)) = ''
       OR t1.industry = 'NULL')
  AND t2.industry IS NOT NULL;

  
-- =========================================================
-- 4. Remove Unusable Rows
-- =========================================================

-- Identify rows with no layoff metrics
Select *
From dbo.layoffs_staging2
Where total_laid_off is Null
AND percentage_laid_off is Null;

-- Remove rows with no analytical value
Delete
From dbo.layoffs_staging2
Where total_laid_off is Null
AND percentage_laid_off is Null;


-- =========================================================
-- Final Cleanup
-- =========================================================

-- Drop helper column used for deduplication
ALTER TABLE dbo.layoffs_staging2
DROP COLUMN row_num;

-- Final cleaned dataset
Select *
From dbo.layoffs_staging2



















