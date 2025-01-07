# **ETL proces datasetu MovieLens**

V tomto repositary je uvedený projekt, ktorý opisuje ETL proces v Snowflake na analýzu dat **MovieLens** datasetu. **Cieľom projektu** je analyzovať filmy na základe **hodnotení** od používateľov. Preferencie určitej **vekovej skupiny** pri výbere filmového **žánru**. Pomocou získaného modelu je možné jednoducho analyzovať, v akú **dennú dobu**, v **aký deň**, v **akom mesiaci** a v **akom roku** najviac ľudí zanechalo nejaké hodnotenia alebo značky a na aký film.

## **1. Úvod a popis zdrojových dát**

Mojím cieľom bolo analyzovať údaje o **filme**, jeho **skóre** a **používateľoch**. Táto analýza umožňuje identifikovať trendy v **preferenciách používateľov**, **najpopulárnejšie** a **najlepšie hodnotené filmy** a **správanie používateľov**.

Zdrojom údajov bola **Cvičná databáza: Hodnotenia filmov pre SQL príkazy - MovieLens**. Dataset údajov obsahuje 8 tabuliek:
- **`movies`**
- **`ratings`**
- **`users`**
- **`occupations`**
- **`genre`**
- **`genre_movies`**
- **`tags`**
- **`age_group`**

---
### **1.1 Dátová architektúra**

### **ERD diagram**
V našom datasete **Cvičná databáza: Hodnotenia filmov pre SQL príkazy - MovieLens** bol uvedený hotový **ERD diagram** pre nášu databazu. Využíva naše zdroje, ktoré sú reprezentované ako relačný model.

<p align="center">
  <img src="https://github.com/geniuusxgod/MovieLens_db_project/blob/main/MovieLens_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 ERD MovieLens</em>
</p>

---
## **2. Dimenzionálny model**

Druhým krokom v projekte bolo vytvorenie **dimenzionálneho modelu(star_schema)**. Pre analýzu som najprv našiel stredovú tabuľku(fact table). V našom prípade je to **`fact_ratings`**, ktorá je prepojená s ostatnými dimenzionálnymi tabuľkami:
- **`dim_users`**: Obsahuje informácie o používateľoch (gender, age_group, occupation).
- **`dim_movies`**: Obsahuje informácie o filmoch (title, release_year).
- **`dim_genres`**: Obsahuje informácie o žánroch (name).
- **`dim_tags`**: Obsahuje informácie o tagoch (tags, created_at).
- **`dim_date`**: Zahrňuje informácie o dátumoch hodnotení (year, quarter, month, week, day).
- **`dim_time`**: Obsahuje podrobné časové údaje (hours, minutes, seconds).

Na obrázku je zobrazený **dimenzionálny model**. Ktorý zobrazuje prepojenia medzi našou **tabuľkou faktov** a ostatnými **dimenziami**, čo poskytuje väčší prehľad.

<p align="center">
  <img src="https://github.com/geniuusxgod/MovieLens_db_project/blob/main/star_schema.png" alt="Star-Schema">
  <br>
  <em>Obrázok 2 Star-schema pre MovieLens</em>
</p>

---
## **3. ETL proces v Snowflake**
Proces ETL sa skladá z troch častí: `E (Extract)`, `T (Transorm)`, `L (Load)`. **Proces ETL** je proces, pomocou ktorého pripravujeme surové údaje na analytické činnosti s nimi.

---
### **3.1 E (Extract)**
Ak chceme načítať naše surové údaje (fromat `.csv`) z datasetu, musíme najprv vytvoriť stage úložiska, v ktorej budú uložené a z ktorej budeme načítavať naše údaje.

#### **Kód vytvorenia *stage úložiska*, *file_format*, *import dat***
```sql
CREATE OR REPLACE STAGE project_stage;
```
Po vytvorení stage úložiska doň načítame naše súbory `.csv`, potom musíme vytvoriť `FILE_FORMAT`, aby sme mohli určiť oddeľovače a ďalšie parametre na správny zápis údajov do tabuliek.

```sql
CREATE OR REPLACE FILE FORMAT CSV
TYPE='CSV'
FIELD_DELIMITER=','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ESCAPE_UNENCLOSED_FIELD = NONE
SKIP_HEADER=1;
```
Teraz môžeme tabuľky naplniť údajmi o používateľoch, filmoch, hodnoteniach, žánroch atď. Údaje importujte pomocou príkazu `COPY INTO`. Ja som importoval do tabuliek pomocou tohto príkazu:

```sql
COPY INTO age_group_staging
FROM @project_stage/age_group.csv
FILE_FORMAT = CSV;
```

V súbore `tags.csv` som mal problematické riadky, takže keď sa vyskytli chyby, použil som príkaz `ON_ERROR = 'CONTINUE'`.

---
### **3.2 T (Transform)**

Vo fáze transformácie boli údaje z tabuliek **vyčistené**, **transformované** a **obohatené**. Hlavným cieľom bolo pripraviť dimenzie a tabuľku faktov, ktoré prispejú k analýze týchto údajov.

Dimenzia `dim_users` obsahuje údaje o používateľoch, napríklad vekové kategórie, zamestnania. Transformácia zahŕňala rozdelenie veku používateľov do kategórií (napr. „18-24“) a pridanie popisov zamestnaní. Táto dimenzia je typu SCD 2, čo umožňuje sledovať historické zmeny v zamestnaní používateľov.
```sql
CREATE OR REPLACE TABLE dim_users AS
SELECT DISTINCT 
us.userId AS dim_userId,
CASE 
        WHEN us.age < 18 THEN 'Under 18'
        WHEN us.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN us.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN us.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN us.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN us.age >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group,
oc.name AS occupation
FROM users_staging us
JOIN age_group_staging ag ON us.age = ag.age_groupId
JOIN occupations_staging oc ON us.occupationId = oc.occupationId;
```
Dimenzia `dim_date` obsahuje údaje o dátume vyhodnotenia. Obsahuje informácie o roku, štvrťroku, mesiaci, týždni a dni. Táto tabuľka je zostavená tak, aby sa dala použiť na časovú analýzu údajov. Je typu SCD 0, pretože údaje v tejto tabuľke sa nemenia a sú statické.
```sql
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_dateId,
    CAST(rated_at AS DATE) AS date,
    DATE_PART(year, rated_at) AS year,  
    DATE_PART(quarter, rated_at) AS quarter,
    DATE_PART(month, rated_at) AS month,
    DATE_PART(week, rated_at) AS week,
    DATE_PART(day, rated_at) AS day
FROM ratings_staging
GROUP BY CAST(rated_at AS DATE), 
         DATE_PART(day, rated_at),  
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at), 
         DATE_PART(week, rated_at), 
         DATE_PART(quarter, rated_at);
```
Dimenzia `dim_time`, podobne ako `dim_date`, umožňuje analyzovať údaje pomocou presného času (v prípade `dim_date` to bolo pomocou presného dátumu). Obsahuje parametre: hodiny, minúty, sekundy. Je to tiež SCD 0, pretože údaje sú statické a nemenia sa.
```sql
CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('HOUR', rated_at)) AS dim_timeId,                   
    rated_at AS timestamp,
    EXTRACT(HOUR FROM rated_at) AS hours,
    EXTRACT(MINUTE FROM rated_at) AS minutes,
    EXTRACT(SECOND FROM rated_at) AS seconds
FROM ratings_staging
GROUP BY rated_at;
```
Dimenzia `dim_tags` obsahuje informácie o tom, čo bolo do značky zapísané, a čas jej vytvorenia. Táto dimenzia bude typu SCD 2, keďže značky možno pridávať a meniť.

Dimenzia `dim_movies` obsahuje údaje o filmoch, napríklad názov filmu, dátum vydania. Je typu SCD 0, pretože údaje v tejto tabuľke sa nemenia a sú statické.

Dimenzia `dim_genres` obsahuje informácie o filmových žánroch. Táto tabuľka je tiež typu SCD 0, pretože údaje zostávajú statické.

Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je hodnota hodnotenia a časový údaj.
```sql
CREATE OR REPLACE TABLE fact_ratings AS
SELECT
    ra.ratingId AS fact_ratingId,
    ra.rating AS rating,
    ra.rated_at AS rated_at,
    dd.dim_dateId AS dim_dateId,
    dt.dim_timeId AS dim_timeId,
    du.dim_userId AS dim_userId,
    dm.dim_movieId AS dim_movieId,
    ts.tagId AS dim_tagId, 
    dg.dim_genreId AS dim_genreId
FROM ratings_staging ra
JOIN dim_date dd ON CAST(ra.rated_at AS DATE) = dd.date
JOIN dim_time dt ON ra.rated_at = dt.timestamp
JOIN dim_movies dm ON ra.movieId = dm.dim_movieId
JOIN dim_users du ON ra.userId = du.dim_userId
JOIN tags_staging ts ON ra.movieId = ts.movieId
JOIN genres_movies_staging gms ON ra.movieId = gms.movieId
JOIN dim_genres dg ON gms.genreId = dg.dim_genreId;
```

---
### **3.3 L (Load)**

Po úspešnom vytvorení tabuľky faktov a dimenzií boli údaje načítané do konečného stavu. V záujme optimalizácie ukladania boli odstránené staging tabuľky.
```sql
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
```

---
## **4. Vizualizácia dát**

Dashboard obsahuje `5 vizualizácií`. Ukazujú, ako pracovať s rozmerovými tabuľkami a analyzovať údaje používateľov, filmy a ich hodnotenia.

<p align="center">
  <img src="https://github.com/geniuusxgod/MovieLens_db_project/blob/main/dashboard.png" alt="Dashboard">
  <br>
  <em>Obrázok 3 Dashboard MovieLens</em>
</p>

---
### **Graf 1: Najobľúbenejšie žánre podľa hodnotení(Top-10)**
Táto vizualizácia nám ukazuje 10 najobľúbenejších žánrov. Pri pohľade na ňu môžeme pochopiť, že používatelia majú najradšej `Fantasy` filmy, pretože majú najvyššie hodnotenie.

```sql
SELECT 
    dg.name AS genre_name,
    AVG(fr.rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_genres dg ON fr.dim_genreId = dg.dim_genreId
GROUP BY dg.name
ORDER BY avg_rating DESC
LIMIT 10;
```
---
### **Graf 2: Filmy s najväčším počtom hodnotení(Top-10)**
Táto vizualizácia zobrazuje 10 najlepších filmov, ktoré používatelia hodnotia najčastejšie a ktoré majú najvyšší počet zostávajúcich hodnotení. Vizualizácia ukazuje, že s veľkým náskokom vyhráva film Hviezdne vojny: Epizóda IV - Nová nádej (1977).

```sql
SELECT 
    dm.title AS movie_title,
    COUNT(fr.fact_ratingId) AS total_ratings
FROM fact_ratings fr
JOIN dim_movies dm ON fr.dim_movieId = dm.dim_movieId
GROUP BY dm.title
ORDER BY total_ratings DESC
LIMIT 10;
```
---
### **Graf 3: Trendy v hodnotení v priebehu rokov**
Na tejto vizualizácii sledujeme počet hodnotení v priebehu rokov (2000-2003). Z grafu vyplýva, že v roku 2000 používatelia zanechali oveľa viac hodnotení ako v ostatných rokoch. Čo znamená, že v tomto roku boli používatelia pri sledovaní filmov aktívnejší.

```sql
SELECT 
    dd.year,
    COUNT(fr.fact_ratingId) AS total_ratings
FROM fact_ratings fr
JOIN dim_date dd ON fr.dim_dateId = dd.dim_dateId
GROUP BY dd.year
ORDER BY dd.year;
```
---
### **Graf 4: Aktivita používateľov počas dňa na základe vekovych kategórii**
Na základe tejto vizualizácie môžeme vidieť, že analyzujeme aktivitu používateľov rozdelenú podľa vekových kategórií počas celého dňa (po hodinách). Pomocou tejto vizualizácie môžeme určiť, v akom čase sú používatelia rôznych vekových kategórií najčastejšie aktívni. 

```sql
SELECT 
    dt.hours AS hour_of_day,
    du.age_group,
    COUNT(fr.fact_ratingId) AS total_ratings
FROM fact_ratings fr
JOIN dim_time dt ON fr.dim_timeId = dt.dim_timeId
JOIN dim_users du ON fr.dim_userId = du.dim_userId
GROUP BY dt.hours, du.age_group
ORDER BY dt.hours ASC, du.age_group;
```
---
### **Graf 5: Pocet hodnoteni podla povolani**
Táto vizualizácia ukazuje, ktorí používatelia s akou profesiou zanechávajú najviac hodnotení filmov. Z vizualizácie vidíme, že najviac filmov hodnotia `Educator`.

```sql
SELECT 
    du.occupation,
    COUNT(fr.rating) AS total_rating
FROM fact_ratings fr
JOIN dim_users du ON fr.dim_userId = du.dim_userId
GROUP BY du.occupation;
```
