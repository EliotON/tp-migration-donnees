import pandas as pd
import pymysql
import datetime
import math

# --- CONFIGURATION DE LA BDD MARIADB ---
# A modifiez si vos identifiants ou le nom voulu pour la base sont différents
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "exemple"
DB_NAME = "cas3_hopital"

# --- INIT LOGS ---
# Dictionnaire qui va alimenter le contenu de notre rapport de reprise
rapport = {
    "Etablissements": {"total": 0, "inserted": 0, "ignored": 0, "error": 0, "errors_list": []},
    "Techniciens": {"total": 0, "inserted": 0, "ignored": 0, "error": 0, "errors_list": []},
    "Equipements": {"total": 0, "inserted": 0, "ignored": 0, "error": 0, "errors_list": []},
    "Interventions": {"total": 0, "inserted": 0, "ignored": 0, "error": 0, "errors_list": []}
}

log_file = None

def init_log_file():
    global log_file
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"log_migration_{timestamp}.txt"
    log_file = open(filename, "w", encoding="utf-8")
    log_file.write("LOG DE MIGRATION DE DONNÉES EN PRODUCTION\n")
    log_file.write(f"Date : {datetime.datetime.now()}\n\n")

# --- FONCTIONS DE LOGGING DE LIGNE ---
def log_insert_success(entity_name, row_id):
    rapport[entity_name]["inserted"] += 1
    log_file.write(f"[{entity_name}] Insertion réussie - ID={row_id}\n")

def log_insert_ignored(entity_name, row_id, reason):
    rapport[entity_name]["ignored"] += 1
    log_file.write(f"[{entity_name}] Ligne ignorée - ID={row_id} - Raison : {reason}\n")

def log_error(entity_name, row_id, error_msg):
    rapport[entity_name]["error"] += 1
    rapport[entity_name]["errors_list"].append({"id": row_id, "error": str(error_msg)})
    log_file.write(f"[{entity_name}] Erreur d'insertion - ID={row_id} - Erreur : {error_msg}\n")

# --- GÉNÉRATION DU RAPPORT COMPLET ---
def generate_report():
    with open("rapport_reprise.txt", "w", encoding="utf-8") as report:
        report.write("RAPPORT DE REPRISE DES DONNÉES\n")
        report.write("==============================\n\n")
        
        for entity_name, stats in rapport.items():
            report.write(f"--- {entity_name.upper()} ---\n")
            report.write(f"Lignes lues (total) : {stats['total']}\n")
            report.write(f"Insérées avec succès : {stats['inserted']}\n")
            report.write(f"Lignes ignorées : {stats['ignored']}\n")
            report.write(f"Lignes en erreur : {stats['error']}\n")
            
            if stats["errors_list"]:
                report.write("Liste des erreurs :\n")
                for err in stats["errors_list"]:
                    report.write(f"  - ID {err['id']} : {err['error']}\n")
            report.write("\n")

# --- CONNEXION ET CRÉATION DE LA BDD MARIADB ---
def setup_database():
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        autocommit=True
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.execute(f"USE {DB_NAME}")
    
    # Création des tables 
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etablissements (
            id_etablissement INT PRIMARY KEY,
            nom VARCHAR(255),
            adresse VARCHAR(255),
            ville VARCHAR(255),
            type VARCHAR(100)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS techniciens (
            id_technicien INT PRIMARY KEY,
            nom VARCHAR(100),
            prenom VARCHAR(100),
            specialite VARCHAR(100),
            email VARCHAR(255)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS equipements (
            id_equipement INT PRIMARY KEY,
            type VARCHAR(100),
            marque VARCHAR(100),
            numero_serie VARCHAR(100),
            date_installation DATE,
            statut VARCHAR(50),
            id_etablissement INT,
            FOREIGN KEY (id_etablissement) REFERENCES etablissements(id_etablissement)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS interventions (
            id_intervention INT PRIMARY KEY,
            id_equipement INT,
            id_technicien INT,
            date_intervention DATE,
            type_intervention VARCHAR(100),
            commentaire TEXT,
            FOREIGN KEY (id_equipement) REFERENCES equipements(id_equipement),
            FOREIGN KEY (id_technicien) REFERENCES techniciens(id_technicien)
        )
    """)
    return conn, cursor

# --- CHARGEMENT ET TRAITEMENTS ---
def process_etablissements(cursor, filepath):
    df = pd.read_csv(filepath)
    for _, row in df.iterrows():
        rapport["Etablissements"]["total"] += 1
        etab_id = row.get("id_etablissement", "Inconnu")
        
        # Nettoyage : Ignorer si valeur manquante sur le nom de l'établissement
        if pd.isnull(row["nom"]) or pd.isnull(row["id_etablissement"]):
            log_insert_ignored("Etablissements", etab_id, "Champs obligatoires manquants")
            continue
            
        try:
            cursor.execute("""
                INSERT INTO etablissements (id_etablissement, nom, adresse, ville, type) 
                VALUES (%s, %s, %s, %s, %s)
            """, (row["id_etablissement"], row["nom"], row["adresse"], row["ville"], row["type"]))
            log_insert_success("Etablissements", etab_id)
        except Exception as e:
            log_error("Etablissements", etab_id, e)

def process_techniciens(cursor, filepath):
    df = pd.read_csv(filepath)
    for _, row in df.iterrows():
        rapport["Techniciens"]["total"] += 1
        tech_id = row.get("id_technicien", "Inconnu")
        
        # Nettoyage 
        if pd.isnull(row["nom"]) or pd.isnull(row["email"]):
            log_insert_ignored("Techniciens", tech_id, "Nom ou Email manquant")
            continue
            
        try:
            cursor.execute("""
                INSERT INTO techniciens (id_technicien, nom, prenom, specialite, email) 
                VALUES (%s, %s, %s, %s, %s)
            """, (row["id_technicien"], row["nom"], row["prenom"], row["specialite"], row["email"]))
            log_insert_success("Techniciens", tech_id)
        except Exception as e:
            log_error("Techniciens", tech_id, e)

def process_equipements(cursor, filepath_v1, filepath_v2):
    # Transformation et Nettoyage de la V2 pour unifier la base de donnée
    df1 = pd.read_csv(filepath_v1)
    df2 = pd.read_csv(filepath_v2)
    
    # Cartographie de transformation de V2 à V1
    mapping = {
        "ref": "id_equipement",
        "designation": "type",
        "fabricant": "marque",
        "sn": "numero_serie",
        "installé_le": "date_installation",
        "état": "statut",
        "etab": "id_etablissement"
    }
    df2_transformed = df2.rename(columns=mapping)
    
    # Fusion des deux datasets
    df = pd.concat([df1, df2_transformed], ignore_index=True)
    
    # Nettoyage profond : suppression des doublons sur l'ID qui feraient crasher la BDD
    df = df.drop_duplicates(subset=['id_equipement'])
    
    for _, row in df.iterrows():
        rapport["Equipements"]["total"] += 1
        equip_id = row.get("id_equipement", "Inconnu")
        
        if pd.isnull(row["id_equipement"]) or pd.isnull(row["numero_serie"]):
            log_insert_ignored("Equipements", equip_id, "ID ou numéro de série manquant")
            continue
            
        try:
            cursor.execute("""
                INSERT INTO equipements (id_equipement, type, marque, numero_serie, date_installation, statut, id_etablissement) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row["id_equipement"], row["type"], row["marque"], row["numero_serie"], row["date_installation"], row["statut"], row["id_etablissement"]))
            log_insert_success("Equipements", equip_id)
        except Exception as e:
            log_error("Equipements", equip_id, e)

def process_interventions(cursor, filepath):
    df = pd.read_csv(filepath)
    for _, row in df.iterrows():
        rapport["Interventions"]["total"] += 1
        inter_id = row.get("id_intervention", "Inconnu")
        
        # S'assurer d'avoir les id de liaison valides
        if pd.isnull(row["id_intervention"]) or pd.isnull(row["id_equipement"]) or pd.isnull(row["id_technicien"]):
            log_insert_ignored("Interventions", inter_id, "Incohérence des identifiants relationnels (clés étrangères)")
            continue
            
        try:
            cursor.execute("""
                INSERT INTO interventions (id_intervention, id_equipement, id_technicien, date_intervention, type_intervention, commentaire) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row["id_intervention"], row["id_equipement"], row["id_technicien"], row["date_intervention"], row["type_intervention"], row["commentaire"]))
            log_insert_success("Interventions", inter_id)
        except Exception as e:
            log_error("Interventions", inter_id, e)

if __name__ == "__main__":
    init_log_file()
    print("Démarrage de la migration globale...")
    
    conn = None
    try:
        conn, cursor = setup_database()
        print(" -> Connexion et Setup Base de données : OK")
        
        # Traiter les données fournies ("data_3/")
        process_etablissements(cursor, "data_3/etablissements.csv")
        print(" -> Table Etablissements : OK")
        
        process_techniciens(cursor, "data_3/techniciens.csv")
        print(" -> Table Techniciens : OK")
        
        process_equipements(cursor, "data_3/equipements_v1.csv", "data_3/equipements_v2.csv")
        print(" -> Table Equipements : OK (avec fusion V1 & V2)")
        
        process_interventions(cursor, "data_3/interventions.csv")
        print(" -> Table Interventions : OK")
        
    except Exception as err:
        print(f"!!! ERREUR CRITIQUE DE BDD !!! : {err}\nVeuillez modifier DB_USER/DB_PASSWORD ou vérifier l'état de votre serveur MariaDB.")
    finally:
        if conn:
            cursor.close()
            conn.close()
            
    # Compilation et écriture des rapports
    generate_report()
    log_file.write("\nFin du journal")
    log_file.close()
    print("\n--- Migration terminée ---")
    print("Le compte-rendu a été formaté dans le fichier 'rapport_reprise.txt'.")
