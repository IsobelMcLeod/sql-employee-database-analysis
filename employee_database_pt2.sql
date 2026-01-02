-- data cleaning -- 

SHOW TABLES IN world_layoffs2;

SELECT *
FROM world_layoffs2.`layoffs-2`;

SELECT* 
FROM world_layoffs2.`layoffs_2`;

DESCRIBE world_layoffs2.`layoffs-2`;

WITH Rolling_total AS 
(
SELECT 
	SUBSTRING(`date`,1,7) AS `MONTH`,
	SUM(total_laid_off) AS total_off
FROM world_layoffs2.`layoffs-2`
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,
SUM(total_off) OVER(order by `MONTH`) AS rolling_total
FROM Rolling_total;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

SELECT *
FROM world_layoffs2.`layoffs-2`
WHERE STR_TO_DATE(`date`, '%m/%d/%Y') BETWEEN '2022-01-01' AND '2023-12-31';

WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised) AS row_num
FROM world_layoffs2.`layoffs-2`
)
SELECT * 
FROM duplicate_CTE
WHERE row_num >1;


SELECT * 
FROM world_layoffs2.`layoffs-2`
WHERE company= 'Oda';

SELECT *
FROM world_layoffs2.`layoffs-2`
WHERE company = 'Casper';

CREATE TABLE `layoffs_staging_2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
)	ENGINE =InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO `layoffs_staging_2`
SELECT *,
       ROW_NUMBER() OVER(
           PARTITION BY company, location, industry, total_laid_off,
                        percentage_laid_off, `date`, stage, country,
                        funds_raised_millions
       ) AS row_num
FROM `layoffs_staging_2`;

DROP TABLE IF EXISTS `layoffs_staging_2`;

CREATE TABLE `layoffs_staging_2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE world_layoffs2.layoffs_staging_2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT DEFAULT NULL,
    percentage_laid_off TEXT,
    date TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT DEFAULT NULL,
    row_num INT
);

INSERT INTO world_layoffs2.layoffs_staging_2
SELECT *,
       ROW_NUMBER() OVER(
           PARTITION BY company, location, industry, total_laid_off,
                        percentage_laid_off, `date`, stage, country,
                        funds_raised
       ) AS row_num
FROM world_layoffs2.`layoffs-2`;

INSERT INTO world_layoffs2.layoffs_staging_2 (
    company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    date,
    stage,
    country,
    funds_raised_millions,
    row_num
)
SELECT 
    company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    date,
    stage,
    country,
    NULL AS funds_raised_millions,   -- because your source table does NOT have this column
    ROW_NUMBER() OVER(
        PARTITION BY company, location, industry, total_laid_off,
                     percentage_laid_off, `date`, stage, country
    ) AS row_num
FROM world_layoffs2.`layoffs-2`;


SELECT `date`
FROM world_layoffs2.`layoffs-2`;

UPDATE world_layoffs2.`layoffs-2`
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE world_layoffs2.`layoffs-2`
MODIFY COLUMN `date` DATE;


SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs2.`layoffs-2`
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;


WITH Company_Year (comapny, years, total_laid_off) AS
( 
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs2.`layoffs-2`
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC
), Company_Year_Rank AS
(SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking 
FROM Company_Year
WHERE years IS NOT NULL
)
select *
FROM Company_Year_Rank
WHERE Ranking <= 5
