import os
import pandas as pd
import numpy as np
import psycopg2 as psy



try :
    co = psy.connect(host = 'londres',
                        database = 'dbjemarcilla',
                        user = 'jemarcilla',
                        password = "achanger")

    curs = co.cursor()







    vision = pd.read_sql('''
                                SELECT
                                        SUM(s.capa_fin) as capacite_totale_region,
                                        r.nom as nom_region
                                FROM
                                        Statistiques s
                                        JOIN Etablissement e ON s.etablissement = e.code
                                        JOIN Departement d ON d.numero = e.departement 
                                        JOIN Region r ON r.nom = d.region
                                WHERE 
                                        s.capa_fin != 'NaN'AND 
                                        s.session = 2022
                                GROUP BY
                                        r.nom;
                            ''', con = co )
    print(vision)
    print()
    print()

    #Création du dossier si nécessaire
    dossier = 'dataframes_de_jean'
    if not os.path.exists(dossier):
        os.mkdir(dossier)

    vision.to_csv('requete1.csv', index=False, sep=",")


    vision = pd.read_sql('''
                                SELECT
                                        count(DISTINCT s.formation) as Nb_formations,
                                        d.numero as numero_departement,
                                        d.nom as nom_departement 
                                FROM
                                        Departement d 
                                        JOIN Etablissement e ON e.departement = d.numero
                                        JOIN Statistiques s ON s.etablissement = e.code
                                GROUP BY
                                        d.numero,
                                        d.nom;
                            ''', con = co )




    #Création du dossier si nécessaire
    dossier = 'dataframes_de_jean'
    if not os.path.exists(dossier):
        os.mkdir(dossier)

    vision.to_csv('requete2.csv', index=False, sep=",")


    vision = pd.read_sql('''
                                SELECT
                                        ((effectifs_candidats_term_generale_boursier_principale / effectifs_candidats_term_general_principale ) * 100 ) AS candidats_boursier,
                                        (((effectifs_candidats_term_general_principale - effectifs_candidats_term_generale_boursier_principale) / effectifs_candidats_term_general_principale) * 100) AS candidats_non_boursiers,
                                        (((effectifs_candidats_proposition_term_generale - effectifs_candidats_boursiers_proposition_term_generale) / effectifs_candidats_proposition_term_generale) * 100) AS acceptes_non_boursiers,
                                        ((effectifs_candidats_boursiers_proposition_term_generale / effectifs_candidats_proposition_term_generale) * 100 ) AS acceptes_boursiers,
                                        ((effectifs_candidats_proposition_term_generale / effectifs_candidats_term_general_principale) * 100) AS pourcentage_admission,
                                        e.nom as nom_etablissement,
                                        s.formation as nom_formation
                                FROM
                                        Statistiques s 
                                        JOIN Etablissement e ON e.code = s.etablissement
                                WHERE 
                                        effectifs_candidats_term_general_principale != 0
                                        AND effectifs_candidats_proposition_term_generale != 0
                                        AND (effectifs_candidats_proposition_term_generale / effectifs_candidats_proposition_term_generale ) != 'NaN'
                                        AND (((effectifs_candidats_proposition_term_generale / effectifs_candidats_term_general_principale )) *100) <= 100
                                GROUP BY
                                        e.nom,
                                        s.formation, 
                                        effectifs_candidats_term_generale_boursier_principale,
                                        effectifs_candidats_term_general_principale,
                                        effectifs_candidats_boursiers_proposition_term_generale,
                                        effectifs_candidats_proposition_term_generale
                                ORDER BY
                                        pourcentage_admission ASC;
                            ''', con = co )




    #Création du dossier si nécessaire
    dossier = 'dataframes_de_jean'
    if not os.path.exists(dossier):
        os.mkdir(dossier)
    
    vision.to_csv('requete3.csv', index=False, sep=",")

    co.commit()
    es = curs.fetchall()

    curs = co.cursor()
        
    curs.close()

except( Exception, psy.DatabaseError ) as error :
    print( error )

finally :
    if co is not None :
        co.close()