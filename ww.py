from cassandra.cluster import Cluster
import pandas as pd
from uuid import uuid4

# Si Cassandra est sur le même hôte Docker, utilise localhost (127.0.0.1)
cluster = Cluster(['127.0.0.1'], port=9042)

session = cluster.connect()

# Exemple : créer un keyspace si besoin
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS covid_keyspace 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

session.set_keyspace('covid_keyspace')

session.execute("""
CREATE TABLE IF NOT EXISTS covid_patients (
    usmer int,
    medical_unit int,
    sex int,
    patient_type int,
    date_died text,
    intubed int,
    pneumonia int,
    age int,
    pregnant int,
    diabetes int,
    copd int,
    asthma int,
    inmsupr int,
    hipertension int,
    other_disease int,
    cardiovascular int,
    obesity int,
    renal_chronic int,
    tobacco int,
    clasiffication_final int,
    icu int,
    patient_id uuid,
    PRIMARY KEY ((patient_id))
);

""")


# 4. Préparer la requête d'insertion (exemple table patients)
insert_query = session.prepare("""
    INSERT INTO covid_patients (
    patient_id,
    usmer,
    medical_unit,
    sex,
    patient_type,
    date_died,
    intubed,
    pneumonia,
    age,
    pregnant,
    diabetes,
    copd,
    asthma,
    inmsupr,
    hipertension,
    other_disease,
    cardiovascular,
    obesity,
    renal_chronic,
    tobacco,
    clasiffication_final,
    icu
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

""")


df = pd.read_csv('data/covid_data.csv')
df['DATE_DIED'] = df['DATE_DIED'].replace('9999-99-99',pd.NaT)
df['DATE_DIED'] = pd.to_datetime(df['DATE_DIED'],format='mixed')
df['covid_result'] = df['CLASIFFICATION_FINAL'].apply(lambda x : 1 if x in [1,2,3] else 0)

for _, row in df.iterrows():
    patient_id = uuid4()
    session.execute(insert_query, (
        patient_id,
        int(row['USMER']),
        int(row['MEDICAL_UNIT']),
        int(row['SEX']),
        int(row['PATIENT_TYPE']),
        str(row['DATE_DIED']) if pd.notna(row['DATE_DIED']) else None,               # chaîne texte
        int(row['INTUBED']),
        int(row['PNEUMONIA']),
        int(row['AGE']),
        int(row['PREGNANT']),
        int(row['DIABETES']),
        int(row['COPD']),
        int(row['ASTHMA']),
        int(row['INMSUPR']),
        int(row['HIPERTENSION']),
        int(row['OTHER_DISEASE']),
        int(row['CARDIOVASCULAR']),
        int(row['OBESITY']),
        int(row['RENAL_CHRONIC']),
        int(row['TOBACCO']),
        int(row['CLASIFFICATION_FINAL']),
        int(row['ICU']),
    ))


print("Import terminé !")
