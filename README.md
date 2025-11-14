Telecom Data Pipeline - Mediation, Rating & Billing System

Pipeline Big Data complet pour la gestion des processus de médiation, tarification et facturation dans le secteur télécom.

Objectifs :

Appliquer les concepts Big Data dans un cas industriel

Implémenter la chaîne complète : médiation → tarification → facturation

Traiter de gros volumes de données en temps réel et batch

Détecter et traiter anomalies et erreurs

Architecture :
Synthetic Data Generation → Streaming Mediation → Batch Rating → Batch Billing → Reporting & Analytics

Stack Technique :

Python, Apache Spark, Apache Kafka

PostgreSQL, Docker, Airflow

Fonctionnalités :

Génération de données synthétiques (voix, SMS, data, anomalies)

Médiation en streaming : ingestion Kafka, normalisation, détection doublons, gestion erreurs

Tarification batch : règles tarifaires, plans produits, promotions, modificateurs temporels et géographiques

Facturation batch : agrégation par client, taxes, cycles mensuels, export JSON/XML/PDF

Reporting & Analytics : tableaux de bord, KPI, monitoring du pipeline

Structure des données :

Base Clients : profils, abonnements, info facturation

Catalogue Produits : services, unités, règles de pricing

Enregistrements : CDR (voix), EDR (data), métadonnées techniques

Prérequis :

Python 3.8+

Apache Spark 3.0+

Apache Kafka 2.8+

PostgreSQL 13+
