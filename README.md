# 🚖 NYC Taxi Data Pipeline

![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-blue)
![dbt](https://img.shields.io/badge/dbt-Transformations-orange)
![Python](https://img.shields.io/badge/Python-Ingestion-yellow)
![Status](https://img.shields.io/badge/Status-In_Development-green)

## 📋 Résumé du Projet

Pipeline ELT moderne conçu pour traiter et analyser plus de **40 millions de trajets** de taxis new-yorkais (2024-2025).
L'objectif est de transformer des données brutes (Parquet) en métriques business actionnables (revenus, patterns de trafic, performance des zones) via une architecture Data Warehouse robuste.

## 🏗️ Architecture Technique

**Architecture Medallion (Multi-couches) :**

1. **Ingestion (Python/S3)** : Chargement automatisé des fichiers Parquet mensuels vers Snowflake.
2. **RAW (Bronze)** : Stockage immuable des données brutes.
3. **STAGING (Silver - dbt)** : Nettoyage, dédoublonnage, typage et tests de qualité (Data Quality).
4. **DATA MARTS (Gold - dbt)** : Tables dimensionnelles et faits optimisés pour l'analytique (BI).

## 🛠️ Stack Technologique

* **Data Warehouse** : Snowflake (Scale-up/Scale-out compute)
* **Transformation** : dbt Core (SQL-based transformation & testing)
* **Langage** : Python 3.9+ & SQL
* **Orchestration** : GitHub Actions (CI/CD)
* **Version Control** : Git (Feature Branch Workflow)

## 🚀 Comment Démarrer

1. Cloner le repo
2. Installer les dépendances : `pip install -r requirements.txt`
3. Configurer les profils dbt (`~/.dbt/profiles.yml`)
4. Lancer le pipeline : `dbt run`
