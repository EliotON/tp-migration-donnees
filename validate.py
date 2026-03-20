import pymysql
import datetime

# --- CONFIGURATION DE LA BDD MARIADB ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "exemple"
DB_NAME = "cas3_hopital"

def setup_db():
    conn = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=True
    )
    return conn, conn.cursor(pymysql.cursors.DictCursor)

def fetch_single_value(cursor, query):
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        return list(result.values())[0]
    return 0

def run_validations():
    report_lines = []
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_lines.append("="*70)
    report_lines.append(f" RAPPORT DE VALIDATION POST-MIGRATION -- {timestamp}")
    report_lines.append("="*70 + "\n")
    
    conn = None
    try:
        conn, cursor = setup_db()
    except Exception as e:
        report_lines.append(f"Erreur de connexion à la base de données : {e}")
        write_report(report_lines)
        return
        
    # --- 1. VOLUMÉTRIE ---
    report_lines.append("--- 1. VÉRIFICATION DES VOLUMES ---")
    tables = ["etablissements", "techniciens", "equipements", "interventions", "equipements_defectueux"]
    for table in tables:
        try:
            vol = fetch_single_value(cursor, f"SELECT COUNT(*) FROM {table}")
            report_lines.append(f" - {table.ljust(25)} : {str(vol).rjust(5)} lignes présentes")
        except Exception:
            report_lines.append(f" - {table.ljust(25)} : ERREUR (table inexistante ou illisible)")
            
    # --- 2. UNICITÉ ---
    report_lines.append("\n--- 2. VÉRIFICATION DE L'UNICITÉ (Clés & Numéros de série) ---")
    queries_unicite = [
        ("Equipements - Numero de Série", "SELECT COUNT(numero_serie) - COUNT(DISTINCT numero_serie) FROM equipements"),
        ("Equipements - Clé Primaire", "SELECT COUNT(id_equipement) - COUNT(DISTINCT id_equipement) FROM equipements"),
        ("Interventions - Clé Primaire", "SELECT COUNT(id_intervention) - COUNT(DISTINCT id_intervention) FROM interventions"),
        ("Etablissements - Clé Primaire", "SELECT COUNT(id_etablissement) - COUNT(DISTINCT id_etablissement) FROM etablissements"),
        ("Techniciens - Clé Primaire", "SELECT COUNT(id_technicien) - COUNT(DISTINCT id_technicien) FROM techniciens")
    ]
    for label, query in queries_unicite:
        try:
            doublons = fetch_single_value(cursor, query)
            statut = "OK" if doublons == 0 else f"ANOMALIE ({doublons} doublons détectés)"
            report_lines.append(f" - {label.ljust(35)} : {statut}")
        except Exception:
            report_lines.append(f" - {label.ljust(35)} : Non testable")
            
    # --- 3. RELATIONS (INTÉGRITÉ RÉFÉRENTIELLE) ---
    report_lines.append("\n--- 3. VÉRIFICATION DES RELATIONS (Clés étrangères) ---")
    queries_relations = [
        ("Equipements -> Etablissements", 
         "SELECT COUNT(*) FROM equipements e LEFT JOIN etablissements et ON e.id_etablissement = et.id_etablissement WHERE et.id_etablissement IS NULL"),
        ("Interventions -> Equipements",
         "SELECT COUNT(*) FROM interventions i LEFT JOIN equipements e ON i.id_equipement = e.id_equipement WHERE e.id_equipement IS NULL"),
        ("Interventions -> Techniciens",
         "SELECT COUNT(*) FROM interventions i LEFT JOIN techniciens t ON i.id_technicien = t.id_technicien WHERE t.id_technicien IS NULL")
    ]
    
    for label, query in queries_relations:
        try:
            orphelins = fetch_single_value(cursor, query)
            statut = "✓ INTACT" if orphelins == 0 else f"❌ VIOLATION ({orphelins} enregistrements sans parent)"
            report_lines.append(f" - {label.ljust(35)} : {statut}")
        except Exception:
            report_lines.append(f" - {label.ljust(35)} : Non testable")
            
    # --- CLOTURE ---
    report_lines.append("\n" + "="*70)
    report_lines.append("CONCLUSION : Examen de post-migration de la BDD achevé.")
    report_lines.append("="*70)
    
    cursor.close()
    conn.close()
    
    write_report(report_lines)

def write_report(lines):
    filename = "rapport_validation.txt"
    # Ecriture dans un fichier log structuré
    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    
    # Affichage en console pour info directe
    print("\n".join(lines))
    print(f"\n📂 Le rapport a bien été horodaté et sauvegardé dans '{filename}'.")

if __name__ == "__main__":
    run_validations()
