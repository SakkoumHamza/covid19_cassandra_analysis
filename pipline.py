# Connexion à Cassandra
from cassandra.cluster import Cluster
import pandas as pd
from uuid import uuid4
import datetime

# Modules de Machine Learning
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

# Connexion à Cassandra (si Cassandra est en local ou via Docker)
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()

# Création du keyspace s'il n'existe pas (bon pour la portabilité)
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS covid_keyspace 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

# Sélection du keyspace
session.set_keyspace('covid_keyspace')

# Création de la table pour stocker les prédictions si elle n'existe pas déjà
session.execute("""
CREATE TABLE IF NOT EXISTS covid_patients (
    patient_id uuid,
    medical_unit int,
    sex int,
    patient_type int,
    intubed int,
    pneumonia int,
    age int,
    diabetes int,
    hipertension int,
    cardiovascular int,
    renal_chronic int,
    covid_result int,
    prediction_date timestamp,
    PRIMARY KEY ((patient_id))
);
""")

# Préparation de la requête d'insertion pour plus de performance
insert_query = session.prepare("""
    INSERT INTO covid_patients (
        patient_id,
        medical_unit,
        sex,
        patient_type,
        intubed,
        pneumonia,
        age,
        diabetes,
        hipertension,
        cardiovascular,
        renal_chronic,
        covid_result,
        prediction_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
""")

# Chargement des données d'entraînement
df = pd.read_csv('data/covid_data.csv')

# Création de la colonne cible : 1 si le patient est COVID+ (classes 1,2,3), sinon 0
df['covid_result'] = df['CLASIFFICATION_FINAL'].apply(lambda x: 1 if x in [1,2,3] else 0)

# Suppression des colonnes peu corrélées ou inutiles pour l'entraînement
drops = [
    'PREGNANT',
    'OTHER_DISEASE',
    'TOBACCO',
    'USMER',
    'INMSUPR',
    'ASTHMA',
    'COPD',
    'OBESITY',
    'ICU',
    'CLASIFFICATION_FINAL',  # remplacée par covid_result
    'DATE_DIED'              # inutile pour la prédiction
]
df = df.drop(columns=drops, axis=1)

# Séparation des features et de la cible
X = df.drop('covid_result', axis=1)
Y = df['covid_result']

# Split des données pour entraînement et test
Xtrain, Xtest, Ytrain, Ytest = train_test_split(X, Y, test_size=0.2, random_state=25)

print('Entrainement du modele')
# Entraînement du modèle Random Forest
model = RandomForestClassifier(criterion='entropy', n_estimators=200, random_state=25)
model.fit(Xtrain, Ytrain)

# Prédiction sur les données de test
Ypred = model.predict(Xtest)

# Affichage des performances du modèle
print('Accuracy score is:', accuracy_score(Ytest, Ypred))
print('Classification report:', classification_report(Ytest, Ypred))

# Nouveaux patients à prédire
new_patients = pd.read_csv('data/new_patients.csv')
new_patients['COVID_RESULT'] = model.predict(new_patients) 


# Insertion des nouveaux enregistrements prédits dans Cassandra
for _, row in new_patients.iterrows():
    patient_id = uuid4()
    session.execute(insert_query, (
        patient_id,
        int(row['MEDICAL_UNIT']),
        int(row['SEX']),
        int(row['PATIENT_TYPE']),
        int(row['INTUBED']),
        int(row['PNEUMONIA']),
        int(row['AGE']),
        int(row['DIABETES']),
        int(row['HIPERTENSION']),
        int(row['CARDIOVASCULAR']),
        int(row['RENAL_CHRONIC']),
        int(row['COVID_RESULT']),     
        datetime.datetime.now()       # Ajout du timestamp de la prédiction
    ))

print("Prédictions insérées avec succès.")
