#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  8 17:49:18 2025

@author: cheikh1
"""


import sqlite3
import pandas as pd
import os

# Connexion à la base de données
# Chercher d'abord DB_PATH dans les variables d'environnement
DB_PATH = os.getenv("DB_PATH")

# Si DB_PATH n'est pas défini, utiliser le chemin relatif par défaut
if not DB_PATH:
    DB_PATH = os.path.join(os.path.dirname(__file__), 'data' , 'bd' , 'database.db')
    # Vérifier si le dossier 'data' existe, sinon le créer
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

print(f"📂 Base de données utilisée : {DB_PATH}")


conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Création des tables
def create_tables():
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS magasins (
        "ID Magasin" INTEGER PRIMARY KEY,
        ville TEXT,
        nombre_salaries INTEGER
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS produits (
        "ID Référence produit" TEXT PRIMARY KEY,
        nom TEXT,
        prix REAL,
        stock INTEGER
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ventes (
        id_vente INTEGER PRIMARY KEY AUTOINCREMENT,
        "ID Référence produit" TEXT,
        "ID Magasin" INTEGER,
        date_vente DATE,
        quantite INTEGER,
        chiffre_affaires REAL,
        FOREIGN KEY("ID Référence produit") REFERENCES produits("ID Référence produit"),
        FOREIGN KEY("ID Magasin") REFERENCES magasins("ID Magasin")
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS analyses (
        id_analyse INTEGER PRIMARY KEY AUTOINCREMENT,
        type_analyse TEXT,
        resultat TEXT,
        date_analyse TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()

# Fonction pour importer les données CSV
def import_csv(file_path, table_name):
    if not os.path.exists(file_path):
        print(f"⚠️ Fichier introuvable : {file_path}")
        return

    df = pd.read_csv(file_path)

    # Vérifier et renommer les colonnes si nécessaire
    if table_name == "magasins":
        df.rename(columns={'Nombre de salariés': 'nombre_salaries'}, inplace=True)

    try:
        # Supprimer les anciennes données avant d'insérer les nouvelles
        cursor.execute(f"DELETE FROM {table_name}")
        conn.commit()

        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"✅ Données importées avec succès dans {table_name}")
    except Exception as e:
        print(f"❌ Erreur lors de l'importation dans {table_name} : {e}")

    conn.commit()


# Création des tables
create_tables()

# Importation des fichiers CSV

#DB_PATH = os.path.join(os.path.dirname(__file__), 'data', 'database.db')
#import_csv("/Users/cheikh1/Documents/projet/data/Donnés brief data engineer - magasins.csv", "magasins")
import_csv(os.path.join(os.path.dirname(__file__), 'data', 'Donnés brief data engineer - magasins.csv'), "magasins")
#import_csv("/Users/cheikh1/Documents/projet/data/Donnés brief data engineer - produits.csv", "produits")
import_csv(os.path.join(os.path.dirname(__file__), 'data', 'Donnés brief data engineer - produits.csv'), "produits")

# Import des ventes en évitant les doublons
def import_ventes(file_path):
    if not os.path.exists(file_path):
        print(f"⚠️ Fichier introuvable : {file_path}")
        return

    df_ventes = pd.read_csv(file_path)
    ventes_importees = 0
    ventes_ignorees = 0

    for _, row in df_ventes.iterrows():
        # Vérifier si la vente existe déjà
        cursor.execute("SELECT COUNT(*) FROM ventes WHERE \"ID Référence produit\"=? AND \"ID Magasin\"=? AND date_vente=?", 
                       (row['ID Référence produit'], row['ID Magasin'], row['Date']))
        
        if cursor.fetchone()[0] == 0:  # Si la vente n'existe pas encore
            # Vérifier si le produit existe
            cursor.execute("SELECT prix FROM produits WHERE \"ID Référence produit\"=?", (row['ID Référence produit'],))
            prix_row = cursor.fetchone()

            if prix_row is not None:
                chiffre_affaires = row['Quantité'] * prix_row[0]
                cursor.execute(
                    "INSERT INTO ventes (\"ID Référence produit\", \"ID Magasin\", date_vente, quantite, chiffre_affaires) VALUES (?, ?, ?, ?, ?)", 
                    (row['ID Référence produit'], row['ID Magasin'], row['Date'], row['Quantité'], chiffre_affaires)
                )
                ventes_importees += 1
            else:
                print(f"⚠️ Produit {row['ID Référence produit']} non trouvé dans la table produits.")
                ventes_ignorees += 1
        else:
            ventes_ignorees += 1

    conn.commit()
    print(f"✅ Importation des ventes terminée : {ventes_importees} ajoutées, {ventes_ignorees} ignorées.")


#import_ventes("/Users/cheikh1/Downloads/Donnés brief data engineer - ventes.csv")
import_ventes(os.path.join(os.path.dirname(__file__), 'data', 'Donnés brief data engineer - ventes.csv'))

# Requêtes SQL d'analyse
def execute_analysis():
    queries = {
        "Chiffre d'affaires total": "SELECT SUM(chiffre_affaires) FROM ventes",
        "Ventes par produit": "SELECT \"ID Référence produit\", SUM(quantite) FROM ventes GROUP BY \"ID Référence produit\"",
        "Ventes par région": "SELECT magasins.ville, SUM(ventes.chiffre_affaires) FROM ventes JOIN magasins ON ventes.\"ID Magasin\" = magasins.\"ID Magasin\" GROUP BY magasins.ville"
    }
    
    for analysis, query in queries.items():
        result = cursor.execute(query).fetchall()
        cursor.execute("INSERT INTO analyses (type_analyse, resultat) VALUES (?, ?)", (analysis, str(result)))
    
    conn.commit()
    print("✅ Analyses enregistrées dans la base de données.")

execute_analysis()

# Fermeture de la connexion à la base de données
conn.close()
print("✅ Connexion à la base de données fermée.")