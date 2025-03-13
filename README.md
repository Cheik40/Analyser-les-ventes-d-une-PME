Projet Analyse des ventes d'un PME

1. Conception de l’architecture
Service 1 : Exécution des scripts
Nom : script_runner
Objectif : Collecter, transformer et importer les données
Entrée : URLs des fichiers CSV fournis par le client
Sortie : Données transformées et insérées dans la base SQLite
Service 2 : Stockage des données
Nom : database_service
Objectif : Héberger une base SQLite contenant les données des ventes, produits et magasins
Entrée : Données importées via le service script_runner
Sortie : Résultats des requêtes SQL
Communication entre services :
script_runner interagit avec database_service via SQLite (fichier .db partagé)
Docker Compose orchestre le démarrage des services

2. Réalisation de l’architecture
a) Création du Dockerfile pour script_runner
Utilisation de python:3.9 comme image de base
Installation des dépendances (pandas, sqlite3, requests)
Exécution d’un script hello-world.py testant la connexion à SQLite
b) Création du fichier docker-compose.yml
Définition des services :
database_service basé sur une image SQLite
script_runner qui dépend de database_service

3. Analyse et structuration des données
a) Conception du schéma de la base de données
Table produits : id_produit (PK), nom, prix
Table magasins : id_magasin (PK), ville
Table ventes : id_vente (PK), id_produit (FK), id_magasin (FK), date_vente, quantité, chiffre_affaires
Table analyses : stockage des résultats des requêtes SQL
b) Création des tables dans SQLite via un script Python

4. Importation des données
Téléchargement des fichiers CSV depuis les URLs fournies
Vérification et insertion des nouvelles données uniquement
Gestion des erreurs (ex : doublons, données corrompues)

5. Analyse des ventes avec SQL
Chiffre d’affaires total
sql

SELECT SUM(chiffre_affaires) FROM ventes;

Ventes par produit
sql
SELECT id_produit, SUM(quantité) FROM ventes GROUP BY id_produit;

Ventes par région
sql

SELECT magasins.ville, SUM(ventes.chiffre_affaires) 
FROM ventes 
JOIN magasins ON ventes.id_magasin = magasins.id_magasin 
GROUP BY magasins.ville;


6. Stockage des résultats des analyses
Insérer les résultats des requêtes dans la table analyses
Un script Python assure l’insertion automatique des résultats après chaque exécution

