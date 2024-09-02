SELECT *
FROM layoffs;

-- Data Cleaning
-- Remove Duplicates
-- Standardize the data
-- Null Values or blank values 
-- Remove any columns
-- it is very risky to remove columns from the actual data so always create a dup of the raw data and work on that instead
-- the code below creates the staging data from the raw that we will be working on but it doesnt contain the data yet
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;
-- this code is what puts all the data in the original into the stage data as well 
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Removing Duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;
-- here we had to create rownum so we can identigy the dups and we had to use all the column headings so we get tje rihgt ones 
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, stage, country, funds_raised_millions,  `date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';
-- here we need to delete one of the dups but we cant del straight away in mysql so wat we do is take e cte and put it in staging2 and then delete


-- to create the table here, i right click on e layoff staging table then Send to SQL Editor > Create Statement to create it then w fill it up next
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, stage, country, funds_raised_millions,  `date`) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

-- Standadizing Data is findng issues with the data and then fixing it

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- here someone has added a . to the usa so we need to clean that so they are all uniform trim trailing was used to get rid of the . at the end
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2 
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT *
FROM layoffs_staging2;

-- if we have to do time series analysis then we have to change the date from text format to month day year so we change the date then we alter it to date
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- here we are going to analyze how to deal with null and blank data,its important to learn how to populate data as well

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;
-- here we had to change all the blanks to nulls b4 working with it
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- here i had to get all the null values and delete them and finally drop the row num to get the final clean dataset
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