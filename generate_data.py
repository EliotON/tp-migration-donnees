import pandas as pd
import random
import datetime

try:
    from faker import Faker
except ImportError:
    print("Veuillez installer faker pour exécuter ce script (ex: pip install faker)")
    exit(1)

fake = Faker('fr_FR')
log_file = None

def init_log_file():
    global log_file
    # Le mode "w" (write) supprime automatiquement le contenu de l'ancien fichier s'il existe
    log_file = open("log.txt", "w", encoding="utf-8")
    log_file.write("LOG DE GÉNÉRATION DE DONNÉES DE TEST\n")
    log_file.write("====================================\n")
    log_file.write(f"Début : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

def generate_etablissements(num_rows):
    data = []
    types_etab = ['Clinique', 'Centre médical', 'CHU', 'EHPAD']
    for i in range(1, num_rows + 1):
        data.append({
            "id_etablissement": i,
            "nom": fake.company() + " Santé",
            "adresse": fake.street_address(),
            "ville": fake.city(),
            "type": random.choice(types_etab)
        })
    df = pd.DataFrame(data)
    df.to_csv("data_3/etablissements.csv", index=False, encoding='utf-8')
    message = f"SUCCÈS - {num_rows} lignes générées et sauvegardées dans etablissements.csv\n"
    print(message.strip())
    log_file.write(message)

def generate_techniciens(num_rows):
    data = []
    specialites = ['Électronique', 'Biomédical', 'Informatique']
    for i in range(1, num_rows + 1):
        data.append({
            "id_technicien": i,
            "nom": fake.last_name(),
            "prenom": fake.first_name(),
            "specialite": random.choice(specialites),
            "email": fake.email()
        })
    df = pd.DataFrame(data)
    df.to_csv("data_3/techniciens.csv", index=False, encoding='utf-8')
    message = f"SUCCÈS - {num_rows} lignes générées et sauvegardées dans techniciens.csv\n"
    print(message.strip())
    log_file.write(message)

def generate_equipements(num_rows, num_etablissements):
    data = []
    types_equip = ['Respirateur', 'Défibrillateur', 'ECG', 'Pousse-seringue', 'Scope']
    statuts = ['Actif', 'En maintenance', 'Hors service']
    for i in range(1, num_rows + 1):
        data.append({
            "id_equipement": i,
            "type": random.choice(types_equip),
            "marque": fake.company(),
            "numero_serie": f"MT-{i:05d}",
            "date_installation": fake.date_between(start_date='-3y', end_date='today'),
            "statut": random.choice(statuts),
            "id_etablissement": random.randint(1, num_etablissements)
        })
    df = pd.DataFrame(data)
    df.to_csv("data_3/equipements.csv", index=False, encoding='utf-8')
    message = f"SUCCÈS - {num_rows} lignes générées et sauvegardées dans equipements.csv\n"
    print(message.strip())
    log_file.write(message)

def generate_interventions(num_rows, num_equipements, num_techniciens):
    data = []
    types_interv = ['Maintenance', 'Réparation', 'Contrôle', 'Remplacement']
    for i in range(1, num_rows + 1):
        data.append({
            "id_intervention": i,
            "id_equipement": random.randint(1, num_equipements),
            "id_technicien": random.randint(1, num_techniciens),
            "date_intervention": fake.date_between(start_date='-2y', end_date='today'),
            "type_intervention": random.choice(types_interv),
            "commentaire": fake.sentence(nb_words=6)
        })
    df = pd.DataFrame(data)
    df.to_csv("data_3/interventions.csv", index=False, encoding='utf-8')
    message = f"SUCCÈS - {num_rows} lignes générées et sauvegardées dans interventions.csv\n"
    print(message.strip())
    log_file.write(message)

if __name__ == "__main__":
    init_log_file()
    print("Début de la génération des données...")
    
    total_etablissements = 15
    total_techniciens = 50
    total_equipements = 300
    total_interventions = 400
    
    generate_etablissements(total_etablissements)
    generate_techniciens(total_techniciens)
    generate_equipements(total_equipements, total_etablissements)
    generate_interventions(total_interventions, total_equipements, total_techniciens)
    
    log_file.write(f"\nFin de la génération : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    log_file.close()
    
    print("Génération terminée avec succès. (Un journal a été créé dans log.txt)")
