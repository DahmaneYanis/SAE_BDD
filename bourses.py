import os
import pandas as pd
import numpy as np
import psycopg2 as psy
import matplotlib.pyplot as plt

try :
    co = psy.connect(host = 'londres',
                    database = 'dbjemarcilla',
                    user = 'jemarcilla',
                    password = "achanger")


   #pourcentages des candidats boursiers et non boursiers  
    df_pct_candidat = pd.read_sql('''SELECT
        (SUM(effectifs_candidats_term_general_principale - effectifs_candidats_term_generale_boursier_principale) / NULLIF(SUM(effectifs_candidats_term_general_principale), 0) * 100 +
         COALESCE(SUM(effectifs_candidats_neo_term_tech_principale - effectifs_candidats_boursiers_term_tech_principale) / NULLIF(SUM(effectifs_candidats_neo_term_tech_principale), 0), 0) * 100 +
         COALESCE(SUM(effectifs_candidats_neo_term_pro_principale - effectifs_candidats_boursiers_neo_term_pro_principale) / NULLIF(SUM(effectifs_candidats_neo_term_pro_principale), 0), 0) * 100) / 3 AS pourcentage_non_brs,
        (COALESCE(SUM(effectifs_candidats_term_generale_boursier_principale) / NULLIF(SUM(effectifs_candidats_term_general_principale), 0), 0) * 100 +
         COALESCE(SUM(effectifs_candidats_boursiers_term_tech_principale) / NULLIF(SUM(effectifs_candidats_neo_term_tech_principale), 0), 0) * 100 +
         COALESCE(SUM(effectifs_candidats_boursiers_neo_term_pro_principale) / NULLIF(SUM(effectifs_candidats_neo_term_pro_principale), 0), 0) * 100) / 3 AS pourcentage_brs
    FROM Statistiques
    WHERE
        effectifs_candidats_term_general_principale != 0
        AND effectifs_candidats_term_general_principale != 'NaN'
        AND effectifs_candidats_neo_term_tech_principale != 0
        AND effectifs_candidats_neo_term_tech_principale != 'NaN'
        AND effectifs_candidats_neo_term_pro_principale != 0
        AND effectifs_candidats_neo_term_pro_principale != 'NaN'
        AND effectifs_candidats_term_generale_boursier_principale != 0
        AND effectifs_candidats_term_generale_boursier_principale != 'NaN'
        AND effectifs_candidats_boursiers_term_tech_principale != 0
        AND effectifs_candidats_boursiers_term_tech_principale != 'NaN'
        AND effectifs_candidats_boursiers_neo_term_pro_principale != 0
        AND effectifs_candidats_boursiers_neo_term_pro_principale != 'NaN'
''', con=co)
    print(df_pct_candidat)

   #camembert  des pourcentages des candidats boursiers et non boursiers
    plt.figure(figsize=(8, 6))
    labels = ['Non boursiers', 'Boursiers']
    plt.pie(df_pct_candidat.iloc[0], labels=labels, autopct='%1.1f%%')
    plt.title('Répartition des candidats boursiers et non boursiers sur parcoursup')
    plt.axis('equal')
    plt.show()

    

    #pourcentages des candidats boursiers et non boursiers ayant reçu une proposition d'admission  
    dt_pct_accepte = pd.read_sql('''SELECT
        (COALESCE(SUM(effectifs_candidats_proposition_term_generale - effectifs_candidats_boursiers_proposition_term_generale) / NULLIF(SUM(effectifs_candidats_proposition_term_generale), 0), 0) * 100 +
         COALESCE(SUM(effectifs_candidats_proposition_term_tech - effectifs_candidats_boursiers_proposition_term_tech) / NULLIF(SUM(effectifs_candidats_proposition_term_tech), 0), 0) * 100 +
         COALESCE(SUM(effectifs_candidats_proposition_term_pro - effectifs_candidats_boursiers_proposition_term_pro) / NULLIF(SUM(effectifs_candidats_proposition_term_pro), 0), 0) * 100) / 3 AS pourcentage_non_brs_accepte,
        (COALESCE(SUM(effectifs_candidats_boursiers_proposition_term_generale) / NULLIF(SUM(effectifs_candidats_proposition_term_generale), 0), 0) * 100 +
         COALESCE(SUM(effectifs_candidats_boursiers_proposition_term_tech) / NULLIF(SUM(effectifs_candidats_proposition_term_tech), 0), 0) * 100 +
         COALESCE(SUM(effectifs_candidats_boursiers_proposition_term_pro) / NULLIF(SUM(effectifs_candidats_proposition_term_pro), 0), 0) * 100) / 3 AS pourcentage_brs_accepte
    FROM Statistiques
    WHERE
        effectifs_candidats_proposition_term_generale != 0
        AND effectifs_candidats_proposition_term_generale != 'NaN'
        AND effectifs_candidats_proposition_term_tech != 0
        AND effectifs_candidats_proposition_term_tech != 'NaN'
        AND effectifs_candidats_proposition_term_pro != 0
        AND effectifs_candidats_proposition_term_pro != 'NaN'
        AND effectifs_candidats_boursiers_proposition_term_generale != 0
        AND effectifs_candidats_boursiers_proposition_term_generale != 'NaN'
        AND effectifs_candidats_boursiers_proposition_term_tech != 0
        AND effectifs_candidats_boursiers_proposition_term_tech != 'NaN'
        AND effectifs_candidats_boursiers_proposition_term_pro != 0
        AND effectifs_candidats_boursiers_proposition_term_pro != 'NaN'
''', con=co)
    
    print(dt_pct_accepte)
   #camembert  des pourcentages des candidats boursiers et non boursiers ayant reçu des propositions d'admission
    plt.figure(figsize=(8, 6))
    labels = ['Non boursiers', 'Boursiers']
    plt.pie(dt_pct_accepte.iloc[0], labels=labels, autopct='%1.1f%%')
    plt.title('Répartition des candidats boursiers et non boursiers acceptés sur parcoursup')
    plt.axis('equal')
    plt.show()



   #pourcentages des candidats boursiers et non boursiers issue d'une terminale générale
    datafr_pct_gen = pd.read_sql('''SELECT
        (COALESCE(SUM( effectifs_candidats_term_general_principale - effectifs_candidats_term_generale_boursier_principale) / NULLIF(SUM( effectifs_candidats_term_general_principale), 0), 0) * 100) AS pct_non_brs_general,
        (COALESCE(SUM(effectifs_candidats_term_generale_boursier_principale) / NULLIF(SUM( effectifs_candidats_term_general_principale), 0), 0) * 100) AS pct_brs_general
    FROM Statistiques
    WHERE
         effectifs_candidats_term_general_principale != 0
        AND  effectifs_candidats_term_general_principale != 'NaN'
        AND effectifs_candidats_term_generale_boursier_principale != 0
        AND effectifs_candidats_term_generale_boursier_principale != 'NaN'
''', con=co)
    print(datafr_pct_gen)

   #camembert  des pourcentages des candidats boursiers et non boursiers issue d'une terminale générale
    plt.figure(figsize=(8, 6))
    labels = ['Non boursiers', 'Boursiers']
    plt.pie(datafr_pct_gen.iloc[0], labels=labels, autopct='%1.1f%%')
    plt.title('Répartition des candidats boursiers et non boursiers en terminale générale')
    plt.axis('equal')
    plt.show()
    

   #pourcentages des candidats boursiers et non boursiers issue d'une terminale téchnologique
    datafr_pct_tech = pd.read_sql('''SELECT
        (COALESCE(SUM( effectifs_candidats_neo_term_tech_principale - effectifs_candidats_boursiers_term_tech_principale) / NULLIF(SUM( effectifs_candidats_neo_term_tech_principale), 0), 0) * 100) AS pct_non_brs_tech,
        (COALESCE(SUM(effectifs_candidats_boursiers_term_tech_principale) / NULLIF(SUM( effectifs_candidats_neo_term_tech_principale), 0), 0) * 100) AS pct_brs_tech
    FROM Statistiques
    WHERE
         effectifs_candidats_neo_term_tech_principale != 0
        AND  effectifs_candidats_neo_term_tech_principale != 'NaN'
        AND effectifs_candidats_boursiers_term_tech_principale != 0
        AND effectifs_candidats_boursiers_term_tech_principale != 'NaN'
''', con=co)
    print(datafr_pct_tech)

   #camembert  des pourcentages des candidats boursiers et non boursiers issue d'une terminale téchnologique
    plt.figure(figsize=(8, 6))
    labels = ['Non boursiers', 'Boursiers']
    plt.pie(datafr_pct_tech.iloc[0], labels=labels, autopct='%1.1f%%')
    plt.title('Répartition des candidats boursiers et non boursiers en terminale technologique')
    plt.axis('equal')
    plt.show()



   #pourcentages des candidats boursiers et non boursiers issue d'une terminale professionnelle
    datafr_pct_pro = pd.read_sql('''SELECT
        (COALESCE(SUM(  effectifs_candidats_neo_term_pro_principale - effectifs_candidats_boursiers_neo_term_pro_principale) / NULLIF(SUM(  effectifs_candidats_neo_term_pro_principale), 0), 0) * 100) AS pct_non_brs_pro,
        (COALESCE(SUM(effectifs_candidats_boursiers_neo_term_pro_principale) / NULLIF(SUM(  effectifs_candidats_neo_term_pro_principale), 0), 0) * 100) AS pct_brs_pro
    FROM Statistiques
    WHERE
          effectifs_candidats_neo_term_pro_principale != 0
        AND   effectifs_candidats_neo_term_pro_principale != 'NaN'
        AND effectifs_candidats_boursiers_neo_term_pro_principale != 0
        AND effectifs_candidats_boursiers_neo_term_pro_principale != 'NaN'
    GROUP BY   effectifs_candidats_neo_term_pro_principale,
             effectifs_candidats_boursiers_neo_term_pro_principale;
''', con=co)
    

    print(datafr_pct_pro)

   #camembert  des pourcentages des candidats boursiers et non boursiers issue d'une terminale professionnelle
    plt.figure(figsize=(8, 6))
    labels = ['Non boursiers', 'Boursiers']
    plt.pie(datafr_pct_pro.iloc[0], labels=labels, autopct='%1.1f%%')
    plt.title('Répartition des candidats boursiers et non boursiers en terminale professionnelle ')
    plt.axis('equal')
    plt.show()





   #moyenne des candidats boursiers et non boursiers en fonction des trois dernières sessions
    datafr_candidats = pd.read_sql('''
        SELECT 
            session,
            (AVG(effectifs_candidats_term_general_principale - effectifs_candidats_term_generale_boursier_principale) +
            AVG(effectifs_candidats_neo_term_tech_principale - effectifs_candidats_boursiers_term_tech_principale) +
            AVG(effectifs_candidats_neo_term_pro_principale - effectifs_candidats_boursiers_neo_term_pro_principale)) / 3 AS moyenne_non_brs,
            (AVG(effectifs_candidats_term_generale_boursier_principale) +
            AVG(effectifs_candidats_boursiers_term_tech_principale) +
            AVG(effectifs_candidats_boursiers_neo_term_pro_principale)) / 3 AS moyenne_brs
        FROM Statistiques
        WHERE
            session IN (2020, 2021, 2022) 
            AND effectifs_candidats_term_general_principale != 'NaN'
            AND effectifs_candidats_neo_term_tech_principale != 'NaN'
            AND effectifs_candidats_neo_term_pro_principale != 'NaN'
            AND effectifs_candidats_term_generale_boursier_principale != 'NaN'
            AND effectifs_candidats_boursiers_term_tech_principale != 'NaN'
            AND effectifs_candidats_boursiers_neo_term_pro_principale != 'NaN' 
        GROUP BY session;
    ''', con=co)


  #moyenne des candidats boursiers et non boursiers acceptés lors des trois dernières sessions
    datafr_acceptes = pd.read_sql('''
        SELECT 
            session,
            (AVG(effectifs_candidats_proposition_term_generale - effectifs_candidats_boursiers_proposition_term_generale) +
            AVG(effectifs_candidats_proposition_term_tech - effectifs_candidats_boursiers_proposition_term_tech) +
            AVG(effectifs_candidats_proposition_term_pro - effectifs_candidats_boursiers_proposition_term_pro)) / 3 AS moy_non_brs_accepte,
            (AVG(effectifs_candidats_boursiers_proposition_term_generale) +
            AVG(effectifs_candidats_boursiers_proposition_term_tech) +
            AVG(effectifs_candidats_boursiers_proposition_term_pro)) / 3 AS moy_brs_accepte
        FROM Statistiques
        WHERE 
            session IN (2020, 2021, 2022)
            AND effectifs_candidats_proposition_term_generale != 'NaN'
            AND effectifs_candidats_proposition_term_tech != 'NaN'
            AND effectifs_candidats_proposition_term_pro != 'NaN'
            AND effectifs_candidats_boursiers_proposition_term_generale != 'NaN'
            AND effectifs_candidats_boursiers_proposition_term_tech != 'NaN'
            AND effectifs_candidats_boursiers_proposition_term_pro != 'NaN' 
        GROUP BY session
        ORDER BY session;
    ''', con=co)

    print(datafr_candidats)
    print(datafr_acceptes)
    #Calcul des moyennes par session pour les candidats non boursiers
    moyenne_non_brs_candidats = datafr_candidats.groupby('session')['moyenne_non_brs'].mean().reset_index()

    #Calcul des moyennes par session pour les candidats acceptés non boursiers
    moy_non_brs_accepte = datafr_acceptes.groupby('session')['moy_non_brs_accepte'].mean().reset_index()

    #Calcul des moyennes par session pour les candidats boursiers
    moyenne_brs_candidats = datafr_candidats.groupby('session')['moyenne_brs'].mean().reset_index()

    #Calcul des moyennes par session pour les candidats acceptés boursiers
    moy_brs_accepte = datafr_acceptes.groupby('session')['moy_brs_accepte'].mean().reset_index()

    #Sessions
    sessions = moyenne_non_brs_candidats['session']

   # Positions des barres sur l'axe x
    x = np.arange(len(sessions))

    #Largeur des barres
    width = 0.20

    #Création de la figure et du subplot
    fig, ax = plt.subplots()

    #Tracé des barres des moyennes non boursiers candidats
    rects1 = ax.bar(x - width, moyenne_non_brs_candidats['moyenne_non_brs'], width, label='Moyenne non boursiers candidats')

    #Tracé des barres des moyennes non boursiers acceptés
    rects2 = ax.bar(x, moy_non_brs_accepte['moy_non_brs_accepte'], width, label='Moyenne non boursiers acceptés')

    #Tracé des barres des moyennes boursiers candidats
    rects3 = ax.bar(x + width, moyenne_brs_candidats['moyenne_brs'], width, label='Moyenne boursiers candidats')

    #Tracé des barres des moyennes boursiers acceptés
    rects4 = ax.bar(x + 2 * width, moy_brs_accepte['moy_brs_accepte'], width, label='Moyenne boursiers acceptés')

    #Ajout des étiquettes des sessions sur l'axe x
    ax.set_xticks(x)
    ax.set_xticklabels(sessions)

    #Ajout des légendes
    ax.legend()

    #Ajout des labels des axes et du titre
    ax.set_ylabel('Moyennes')
    ax.set_xlabel('Sessions')
    ax.set_title('Moyennes en fonction des sessions')

    #Affichage du graphique
    plt.show()


except (Exception , psy. DatabaseError ) as error :
    print ( error )
finally :
    if co is not None:
        co.close ()
