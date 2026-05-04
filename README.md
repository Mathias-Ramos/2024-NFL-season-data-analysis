# 2024 NFL Season: a Players and Strategy data analysis (Arizona Cardinals) 🏈

[![Lien du Projet](https://img.shields.io/badge/Consulter-Le_Projet-red)](#)
[![Domaine](https://img.shields.io/badge/Domaine-Data_Analytics-blue)](#)
[![Outils](https://img.shields.io/badge/Outils-SQL_|_dbt_|_Tableau_|_Google_Sheets_|_Looker_Studio-orange)](#)

## Présentation du Projet
Ce projet transforme les données brutes de la NFL en avantages tactiques pour les **Arizona Cardinals**. L'objectif est de fournir au staff technique un outil d'aide à la décision basé sur la donnée pour optimiser le recrutement et les stratégies de match en temps réel.
- Durée : 10 jours
- Equipe : 4 personnes

## Problématiques Business
*   **Optimisation du Roster :** Identification des joueurs sous-performants via une analyse comparative des métriques de la ligue.
*   **Tactique de Match :** Prédiction des tendances adverses en situations critiques, comme les 3èmes tentatives.
*   **Efficacité Opérationnelle :** Analyse du rendement par yard selon les formations, notamment la comparaison entre "Under Center" et "Shotgun".

## Stack Technique
*   **Langages :** SQL.
*   **Data Warehouse :** Google BigQuery
*   **Data Cleaning :** Google Sheets, dbt
*   **Data Transformation :** dbt (Data Build Tool) utilisant une architecture en couches (Staging, Intermediate, Marts).
*   **Sources de Données :** Kaggle, Pro-Football-Reference et NFLSavant.
*   **Visualisation & Reporting :** Looker Studio.

## Insights Clés & Résultats

L'ensemble des dashboards finaux sont accessibles à l'adresse suivante : https://datastudio.google.com/reporting/9e5cad15-af7e-4057-ac06-7f198721c7d3. Vous trouverez ci-dessous quelques insights mis en avant grâce aux analyses.

### 1. Analyse de la Performance (Player Analytics)
*   **Kyler Murray (QB) :** Classé 11ème sur 45 QB, avec une performance en yards par match supérieure de 9,7 % à la moyenne de la ligue, son taux de turnover est néanmoins bie nplus élevé que la moyenne de la ligue.
*   **James Conner (RB) :** Malgré une productivité élevée (+12,4 % vs ligue), l'analyse identifie un besoin de succession dû à son âge de 30 ans.
*   **Kyzir White (LB) :** Classé dans le Top 7 des linebackers avec une efficacité de plaquage 13,5 % plus élevée que la moyenne.

### 2. Stratégie de Jeu (Game Theory)
*   **Optimisation Offensive :** Les formations "Under Center" pour les passes longues vers le milieu génèrent en moyenne 17,41 yards, malgré une utilisation moindre que le "Shotgun".
*   **Analyse Prédictive (vs Detroit Lions) :** Identification d'une vulnérabilité sur les 3èmes tentatives avec un taux de sack adverse de 8,04 %, soit 50,5 % au-dessus de la moyenne.

## Architecture Data
Le projet repose sur une structure modulaire pour garantir la fiabilité des KPIs :
1.  **Staging :** Nettoyage et typage des données sources brutes.
2.  **Intermediate :** Jointures complexes et agrégation des statistiques situationnelles.
3.  **Marts :** Tables de faits (`fct_game_recommendations`) et dimensions (`dim_player_rankings`) prêtes pour l'analyse décisionnelle.

## Recommandations Actionnables
*   **Défense :** Prioriser la formation **Nickel 4-2 (Cover 6)** sur les situations de 1ère & 10 pour contrer la probabilité de course adverse.
*   **Attaque :** Augmenter les appels de jeu vers les extrémités (Ends) pour maximiser le gain moyen par tentative par rapport aux courses centrales.

---

### Contact
*   **LinkedIn :** https://www.linkedin.com/in/mathiasramos/
*   **GitHub :** https://github.com/Mathias-Ramos
