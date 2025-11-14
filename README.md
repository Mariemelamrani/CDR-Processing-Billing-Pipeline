TELECOM DATA PIPELINE - MEDIATION, RATING & BILLING SYSTEM

**DESCRIPTION**
Pipeline Big Data complet pour la gestion des processus de médiation, tarification et facturation dans le secteur télécom.

**OBJECTIFS**
- Appliquer les concepts Big Data dans un cas industriel réel
- Implémenter la chaîne complète : médiation → tarification → facturation
- Traiter de gros volumes de données en temps réel et batch
- Détecter et gérer les anomalies et erreurs

**ARCHITECTURE**
Pipeline principal : Synthetic Data Generation → Streaming Mediation → Batch Rating → Batch Billing → Reporting & Analytics

**STACK TECHNIQUE**
- Python - Langage principal de développement
- Apache Spark - Traitement batch et streaming
- Apache Kafka - Ingestion en temps réel
- PostgreSQL - Base de données clients et catalogues
- Docker - Containerisation et déploiement
- Airflow - Orchestration des workflows

**FONCTIONNALITÉS PRINCIPALES**

**1. Génération de données synthétiques**
   - Production de CDR/EDR réalistes (voix, SMS, données)
   - Génération d'anomalies contrôlées (doublons, champs manquants, données corrompues)
   - Contrôle du volume et distribution des services

**2. Médiation en streaming**
   - Ingestion temps réel via Kafka
   - Normalisation et validation des données
   - Détection des doublons et gestion des erreurs
   - Filtrage des enregistrements invalides

**3. Tarification batch**
   - Application des règles tarifaires complexes
   - Gestion des plans produits et promotions
   - Modificateurs temporels et géographiques
   - Calcul des coûts par service

**4. Facturation batch**
   - Agrégation des charges par client et cycle
   - Application des taxes et frais réglementaires
   - Génération de factures (JSON, XML, PDF)
   - Gestion des quotas et unités gratuites

**5. Reporting & Analytics**
   - Tableaux de bord de consommation client
   - KPI revenue et performance business
   - Monitoring des performances du pipeline
   - Analytics des patterns d'utilisation

**STRUCTURE DES DONNÉES**
- Base Clients : profils, abonnements, informations de facturation
- Catalogue Produits : services, unités, règles de pricing
- Enregistrements : CDR (voix), EDR (data), métadonnées techniques

**PRÉREQUIS SYSTÈME**
- Python 3.8+
- Apache Spark 3.0+
- Apache Kafka 2.8+
- PostgreSQL 13+


