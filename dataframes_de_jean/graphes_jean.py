import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import psycopg2 as psy



# Ouverture du curseur 
try :
    co = psy.connect(host = 'londres',
                        database = 'dbjemarcilla',
                        user = 'jemarcilla',
                        password = "achanger")

    curs = co.cursor()


    # Première requête sur le nombre de places dans des formations par région 
    df1 = pd.read_sql('''
                                SELECT
                                        SUM(s.capa_fin) as capacite_totale_region,psal
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


    # Seconde requête sur les proportions de boursiers et non boursiers candidats et acceptés 
    # en fonction de taux d'admission de la formation 
    df2 = pd.read_sql('''
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


    # Cette requête n'est finalement pas utilisée mais permet de récupérer l'ensemble des coordonnées 
    # géographiques des établissements. 
    # Elle aurait servie pour afficher la position des établissements sur une carte de France
    df4 = pd.read_sql('''
                        SELECT
                                e.geo_localisation
                        FROM
                                Etablissement e;''', con = co)


# On ferme ensuite le curseur et la connexion à la base de données, les dataframes étant bien récupérés
    co.commit()
    es = curs.fetchall()
    curs = co.cursor()
    curs.close()

except( Exception, psy.DatabaseError ) as error :
    print( error )

finally :
    if co is not None :
        co.close()



# ===================
# Premier graphique : 
# ===================


# Extraire les données nécessaires pour le graphique
# On prend le pourcentage d'admission des formations ( déjà trié par ordre croissant ) en abcsisse 
x = df2['pourcentage_admission']
# On met les autres données en ordonnées pour les représenter sous forme de courbes
y1 = df2['candidats_boursier']
y2 = df2['candidats_non_boursiers']
y3 = df2['acceptes_boursiers']
y4 = df2['acceptes_non_boursiers']

# On divise les données en groupes de 4 pour une meilleure lisibilité sur le graphique 
x_grouped = x.groupby(pd.cut(x, bins=range(0, 101, 4))).mean()
y1_grouped = y1.groupby(pd.cut(x, bins=range(0, 101, 4))).mean()
y2_grouped = y2.groupby(pd.cut(x, bins=range(0, 101, 4))).mean()
y3_grouped = y3.groupby(pd.cut(x, bins=range(0, 101, 4))).mean()
y4_grouped = y4.groupby(pd.cut(x, bins=range(0, 101, 4))).mean()

# On créé un objet figure et des sous-plots pour les différentes courbes
fig, ax = plt.subplots()

# Ajouter les courbes sur le graphique
ax.plot(x_grouped, y1_grouped, label='Candidats boursiers')
ax.plot(x_grouped, y2_grouped, label='Candidats non boursiers')
ax.plot(x_grouped, y3_grouped, label='Admis boursiers')
ax.plot(x_grouped, y4_grouped, label='Admis non boursiers')

# On ajoute un titre et des labels d'axes
ax.set_title('Evolution des boursiers et non boursiers en fonction de pourcentage d\'admission')
ax.set_xlabel('Pourcentage d\'admission')
ax.set_ylabel('Proportion de candidats')
ax.legend()

# Affichage du graphique
plt.show()


# ===================
# Second graphique :
# ===================

# Toutes les chaînes de caractères de la base de données étant stockées sous forme d'entiers 
# il faut donc avant toute chose les reconvertir en chaînes de caractères. 
# Pour cela, on récupère les dictionnaires ( stockés dans des fichiers CSV ) nécessaires à la traduction
#  des entiers en chaînes de caractères ( réalisé dès le début au niveau du traitement des données ) 
# et on créé les dictionnaires permettant d'effectuer les traductions. 

# On commence par définir la liste des dictionnaires que l'on veut charger
tab = ['region_etab_aff']

print()
# # On définit le chemin vers les fichiers csv contenant les dictionnaires
path = "./dictionnaires_traduction/"

# # On parcours tous les fichiers contenus dans le dossier 
for filename in os.listdir(path) :
    # On récupère pour chaque fichier le nom de la colonne du dataframe principal :
    # Dans ce sens, d'abord on enlève l'extension du fichier ".csv"
    col = filename[:-4]

    # Ensuite on enlève le "traduction" présent en début des nom de fichiers
    col = col.replace("traduction_", "") # retire le préfixe "traduction_"

    # On sélectionne les fichiers .csv présents dans le dossier
    if filename.endswith(".csv") and col in tab :

        print( " -> Dictionnaire " + col + " en cours de chargement en python ")

        # Pour chaque fichier, on le transfère dans un dataframe
        df = pd.read_csv(path + filename, delimiter=";")

        # On récupère la liste de valeurs uniques pour la colonne principale 
        values = df[col].unique()
        # On déclare un dictionnaire
        dico = {}
        
        # On va parcourir les différentes valeurs uniques. On dit que le premier numero remplaçant un string est 1
        for i, value in enumerate(values, start=1):
            # Le dictonnaire prend un nouveau couple clé / valeur
            dico[i] = value
            # toutes les colonnes possédant la valeur en question prennent le nouveau numero
            df[col] = df[col].replace(value, i)
            # On renomme le dictionnaire en mode " traduction_nomColonne"

        globals()["traduction_" + col] = dico


# Pour chaque valeur INT dans la colonne nom_region du dataframe, on la remplace par 
# la valeur string correspondante stockée dans le dictionnaire traduction_region_etab_eff
# tout juste créé au dessus. 
for i in range(len(df1['nom_region'])):
    df1.loc[i, 'nom_region'] = traduction_region_etab_aff[df1.loc[i, 'nom_region'] ]


# On donne aux régions métropolitaines leur numéro administratif officiel 
nouvelle_colonne = pd.Series([75, 93, 27, 76, 32, 84, 11, 
                                53, 44, 52, 24, 28, 94 ])
df1['code'] = nouvelle_colonne
df1 = df1.dropna(subset = ['code'])

# On trie les régions par ordre alphabétique croissant
df1 = df1.sort_values('nom_region')

# On récupère un dataframe geoPandas contenant les informations sur une carte de France et ses régions
regions = gpd.read_file("https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/regions.geojson")

# On remplace le "Île-de-France" ( initialement le premier dans la liste des régions du dataframe, d'où le "0")
# car sinon le tri par ordre alphabétique a des soucis à trier le "^" de "Île"
regions.loc[0, 'nom'] = "Ile-de-France"
# On trie donc les régions par ordre alphabétique du dataframe récupéré au dessus 
regions = regions.sort_values('nom')

# Maintenant que les 2 dataframes sont triés, les mêmes régions se retrouvent aux mêmes numéros de colonnes
# dans les 2 dataframes.
# On va donc récupérer la capacité en places dans les formations de chaque région dans le premier dataframe issu
# de la requête sql pour ensuite mettre ces données dans le second dataframe contenant la carte de France et ses régions 
liste = []
for i in range(len(df1['nom_region'])) :
        liste.append(df1.loc[i, 'capacite_totale_region']) 

# On ajoute donc les capacités des régions au dataframe 
regions['capacite_region'] = liste


# On génère la carte de France 
fig, ax = plt.subplots(figsize=(10, 10))
regions.plot(column="capacite_region", cmap="YlGn", linewidth=0.8, ax=ax, edgecolor="0.8")
ax.axis("off")

# # On ajoute pour chaque région sa capacité pour l'afficher sur la carte 
for idx, row in regions.iterrows():
    plt.annotate(text=row["capacite_region"], xy=row["geometry"].centroid.coords[0], ha="center")

# On met un titre et on affiche la carte 
plt.title("Capacité totale de places dans le supérieur en première année  par région en France")
plt.show()


# =====================
# Troisième graphique :
# # =================== 

# création d'un GeoDataFrame à partir des données de géolocalisation issues de la requête 
geometry = [Point(xy) for xy in df4["geo_localisation"].dropna().apply(lambda x: [float(i) for i in x.split(",")])]
points = gpd.GeoDataFrame(df4.dropna(subset=["geo_localisation"]), geometry=geometry, crs="EPSG:4326")

# On supprime les valeurs manquantes 
df4 = df4.dropna() 

# On définit une latitude et une longitude distinctes pour chaque localisation
df4["lon"], df4["lat"] = df4["geo_localisation"].str.split(",", n=1).str

# On ajoute une colonne geometry qui stocke dans le format demandé par geoPandas 
# pour afficher des points sur une carte 
df4["geometry"] = gpd.points_from_xy(df4["lon"].astype(float), df4["lat"].astype(float))
points = gpd.GeoDataFrame(df4, geometry="geometry")

# On crée la carte de France ( on réutilise la même carte que la carte précédente )
ax = region.plot(color="white", edgecolor="black")
ax.set_xlim([-5, 10])
ax.set_ylim([41, 52])

# ajout des points sur la carte
points.plot(ax=ax, markersize=50, color="red")

# affichage de la carte
plt.show()

# Malheureusement, après de très nombreux essais et alors que les coordonnées sont censées 
# être dans le bon format, les points représentant les coordonnées ne s'affichent pas sur la carte 


