import pandas as pd
import psycopg2 as psy

data = pd.read_csv("donnees_traitees_finales.csv", delimiter=";")
df = pd.DataFrame(data)
df = df.drop_duplicates()

print(df.columns)
# Pour remplacer les valeurs NULL par 0 :
#----------------------------------------
# df = df.fillna(0)
# Mais on ne l'utilise pas dans le cas présent. 

try :
    co = psy.connect(host = 'londres',
                    database = 'dbjemarcilla',
                    user = 'jemarcilla',
                    password = "achanger")

    curs = co.cursor()


# Création des tables : 

    #print(type(df))
    #test = df[['region_etab_aff', 'acad_mies']].drop_duplicates()
    #print(test)

    curs.execute('''DROP TABLE IF EXISTS Region;''')
    curs.execute('''
                    CREATE TABLE Region
                    (
                        nom numeric(10) PRIMARY KEY
                    );
                ''')



    #for row in df.itertuples() :
      #  print(type(row.region_etab_aff))
     #   curs.execute( '''INSERT INTO Region VALUES (%s);''', (row.region_etab_aff))
        
            

    

    # curs.execute('''DROP TABLE IF EXISTS Departement;''')0.
    # curs.execute('''
    #                 CREATE TABLE Departement
    #                 (
    #                     numero numeric(2) PRIMARY KEY,
    #                     nom numeric(10),
    #                     region numeric(10) REFERENCES Region
    #                 );
    #             ''')

    # for row in df.itertuples() :
    #             curs.execute( '''INSERT INTO Departement VALUES (%s, %s, %s);''',
    #                 (row.dep, row.dep_lib, row.region_etab_aff))
    



    # curs.execute('''DROP TAtry :
    co = psy.connect(host = 'londres',
                    database = 'dbjemarcilla',
                    user = 'jemarcilla',
                    password = "achanger")

    curs = co.cursor()BLE IF EXISTS Etablissement;''')
    # curs.execute('''
    #                 CREATE TABLE Etablissement
    #                 (
    #                     code numeric(10) PRIMARY KEY, 
    #                     session numeric(4),
    #                     coordonnees numeric(10)
    #                     dep numeric(10) REFERENCES Departement
    #                 );
    #                 ''')
    # for row in df.itertuples() :
    #             curs.execute( '''INSERT INTO Etablissement VALUES (%s, %s, %s, %s);''',
    #                 (row.code_uai, row.session, row.g_olocalisation_des_formations , row.dep))
    



    # curs.execute('''DROP TABLE IF EXISTS Statistiques;''')
    # curs.execute('''
    #                 CREATE TABLE Statistiques
    #                 (
    #                     etablissement numeric(10) REFERENCES Etablissement,
    #                     formation numeric(10) REFERENCES Formation,
    #                     PRIMARY KEY (etablissement, formation)
    #                 );
    #             ''')
    # for row in df.itertuples() :
    #             curs.execute( '''INSERT INTO Statistiques VALUES (%s, %s);''',
    #                 (row.cod_uai, row.form_lib_voe_acc))
                

    


    # curs.execute('''DROP TABLE IF EXISTS Formation;''')
    #                    region numeric(10) REF
    #                 CREATE TABLE Formation
    #                 (
    #                     nom numeric(10) PRIMARY KEY,
    #                     contrat numeric(2),
    #                     selectif numeric(1),
    #                 );
    #             ''')

    # for row in df.itertuples() :
    #             curs.execute( '''INSERT INTO Etablissement VALUES (%s, %s, %s);''',
    #                 (row.form_lib_voe_acc, row.contrat_etab, row.select_form))




    co.commit()
    res = curs.fetchall()
    #print(res)
    try :
    co = psy.connect(host = 'londres',
                    database = 'dbjemarcilla',
                    user = 'jemarcilla',
                    password = "achanger")

    curs = co.cursor()
    
    curs.close()

except( Exception, psy.DatabaseError ) as error :
    print( error )

finally :
    if co is not None :
        co.close()