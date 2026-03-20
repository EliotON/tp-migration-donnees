import pandas as pd
import pymysql
import datetime

# --- CONFIGURATION DE LA BDD MARIADB ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "exemple"
DB_NAME = "cas3_hopital"

def setup_db():
    conn = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, autocommit=True
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.execute(f"USE {DB_NAME}")
    
    # Création de la table pour ré-injecter les lignes corrigées ou valides
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS equipements_defectueux (
            id_equipement INT PRIMARY KEY,
            nom VARCHAR(255),
            categorie VARCHAR(100),
            date_achat DATE,
            prix FLOAT
        )
    """)
    return conn, cursor

def detect_and_inject(filepath):
    # Ouverture du journal des anomalies ("log_reprise.txt")
    log_file = open("log_reprise.txt", "w", encoding="utf-8")
    log_file.write(f"LOG DE REPRISE DES ANOMALIES - {datetime.datetime.now()}\n")
    log_file.write("="*60 + "\n\n")

    conn = None
    try:
        conn, cursor = setup_db()
    except Exception as e:
        print(f"Erreur de connexion à la base de données : {e}")
        return

    # Chargement du fichier défectueux
    df = pd.read_csv(filepath)
    
    valides = 0
    isolees = 0
    corrigees = 0
    
    for _, row in df.iterrows():
        equip_id = row.get("id_equipement")
        nom = row.get("nom")
        cat = row.get("categorie")
        date_a = str(row.get("date_achat"))
        prix = row.get("prix")
        
        erreurs = []
        is_corrected = False
        final_prix = None
        
        # 1. Vérification des champs manquants
        if pd.isnull(nom) or pd.isnull(cat):
            erreurs.append("Nom ou Catégorie manquant (impossible à corriger)")
            
        # 2. Vérification des formats de date aberrants (ex: 2023-15-01)
        valid_date = None
        try:
            valid_date = datetime.datetime.strptime(date_a, "%Y-%m-%d").date()
        except ValueError:
            erreurs.append(f"Format de date hors calendrier ({date_a})")
            
        # 3. Vérification des anomalies de prix (ex: prix négatif ou nul)
        try:
            prix_float = float(prix)
            if prix_float < 0:
                # CORRECTION : on ramène le prix négatif en prix positif (erreur de signe probable)
                final_prix = abs(prix_float)
                is_corrected = True
                log_file.write(f"INFO CORRECTION - ID: {equip_id} (Prix {prix_float} corrigé en {final_prix})\n")
            elif prix_float == 0:
                erreurs.append("Le prix ne peut être égal à 0")
            else:
                final_prix = prix_float
        except ValueError:
            erreurs.append(f"Prix non numérique ({prix})")
            
        # --- DÉCISION (Isoler ou Réinjecter) ---
        if erreurs:
            isolees += 1
            log_file.write(f"ERREUR ISOLÉE - Ligne ID: {equip_id} sautée car : {', '.join(erreurs)}\n")
        else:
            if is_corrected:
                corrigees += 1
            else:
                valides += 1
                
            log_file.write(f"SUCCÈS - Ligne ID: {equip_id} prête à être réinjectée.\n")
            
            # Réinjection dans MariaDB
            try:
                cursor.execute("""
                    INSERT INTO equipements_defectueux (id_equipement, nom, categorie, date_achat, prix)
                    VALUES (%s, %s, %s, %s, %s)
                """, (equip_id, nom, cat, valid_date, final_prix))
            except Exception as e:
                log_file.write(f"ERREUR BDD - Impossible d'insérer ID: {equip_id} : {e}\n")

    # Bilan final
    total_reinjecte = valides + corrigees
    log_file.write(f"\n--- BILAN DE LA REPRISE ---\n")
    log_file.write(f"Lignes parfaitement valides réinjectées : {valides}\n")
    log_file.write(f"Lignes corrigées automatiquement puis réinjectées : {corrigees}\n")
    log_file.write(f"Total des lignes réinjectées : {total_reinjecte}\n")
    log_file.write(f"Lignes isolées et exclues : {isolees}\n")
    
    log_file.close()
    if conn:
        cursor.close()
        conn.close()
        
    print(f"Opération de reprise terminée !")
    print(f" -> {total_reinjecte} réinjectées (dont {corrigees} corrigées), {isolees} définitivement isolées.")
    print(" -> Les détails ligne par ligne sont disponibles dans 'log_reprise.txt'.")

if __name__ == "__main__":
    # Nom du fichier fourni (s'il s'appelle différemment sur votre machine, ajustez le nom)
    file_path = "cas3_equipements_defectueux.csv"
    detect_and_inject(file_path)
