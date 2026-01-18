----- Data Cleaning


SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null values or blank values
-- 4. Remove Any Columns or Rows

CREATE TABLE layoffs_staging
LIKE layoffs; 

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


 SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
 FROM layoffs_staging;

WITH duplicate_cte AS    # 2 is duplicates in table
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, 
location, industry, total_laid_off, percentage_laid_off, `date`
, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *                 # duplicates which needs to be remove
FROM layoffs_staging
WHERE company = 'Casper'; 


WITH duplicate_cte AS   # not working some time to delet it
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, 
location, industry, total_laid_off, percentage_laid_off, `date`
, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `layoffs_staging2` (                      #edited
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT                                         #edited
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, 
location, industry, total_laid_off, percentage_laid_off, `date`
, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE                # deleted duplicates
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *               # checked all clear
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *               
FROM layoffs_staging2;

-- Standardizing data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2   # after runig this all crypto.. conver into 'crypto'
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT *
FROM layoffs_staging2;

SELECT DISTINCT location # checking all row is good or not
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country # checking all row is good or not
FROM layoffs_staging2    
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2            # it will fix it
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,        # formate it in 'M-D-Y'
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2  #updated all 
SET `date` =  STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2  # if i select layoffs_staging2 we can see date--date in info.
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2        # it show NULL values in following rows selected
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = null        # convert blank box to null valus
WHERE industry = '';


SELECT *
FROM layoffs_staging2   # shows Null from industry
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2           #this one have 3 null valus
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2   #try to remove null but it's blank so no working
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL; 

SELECT *
FROM layoffs_staging2;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


