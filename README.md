# **ETL proces datasetu MovieLens**

V tomto repositary je uvedený projekt, ktorý opisuje ETL proces v Snowflake na analýzu dat **MovieLens** datasetu. **Cieľom projektu** je analyzovať filmy na základe **hodnotení** od používateľov. Preferencie určitej **vekovej skupiny** pri výbere filmového **žánru**. Pomocou získaného modelu je možné jednoducho analyzovať, v akú **dennú dobu**, v **aký deň**, v **akom mesiaci** a v **akom roku** najviac ľudí zanechalo nejaké hodnotenia alebo značky a na aký film.

## **1. Úvod a popis zdrojových dát**

Mojím cieľom bolo analyzovať údaje o **filme**, jeho **skóre** a **používateľoch**. Táto analýza umožňuje identifikovať trendy v **preferenciách používateľov**, **najpopulárnejšie** a **najlepšie hodnotené filmy** a **správanie používateľov**.

Zdrojom údajov bola **Cvičná databáza: Hodnotenia filmov pre SQL príkazy - MovieLens**. Dataset údajov obsahuje 8 tabuliek:
- `movies`
- `ratings`
- `users`
- `occupations`
- `genre`
- `genre_movies`
- `tags`
- `age_group`

---
### **1.1 Dátová architektúra**

### **ERD diagram**
V našom datasete **Cvičná databáza: Hodnotenia filmov pre SQL príkazy - MovieLens** bol uvedený hotový **ERD diagram** pre nášu databazu. Využíva naše zdroje, ktoré sú reprezentované ako relačný model.

<p align="center">
  <img src="https://github.com/geniuusxgod/MovieLens_db_project/MovieLens_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 ERD MovieLens</em>
</p>
