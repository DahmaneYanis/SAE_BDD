import os
import pandas as pd
import numpy as np


# Importation, chargement et traitement des données dans des dataframes :
# -----------------------------------------------------------------------

# Ouverture du fichier csv Parcoursup 2022
parcoursup_2022_df = pd.read_csv("jeux_donnees/parcoursup_2022.csv", delimiter=";")
# Ajout d'une colonne "Matiere" avec la valeur "Math" pour tous les élèves
parcoursup_2022_df["Annee"] = "2022"

            

# Ouverture du fichier csv Parcoursup 2021
parcoursup_2021_df = pd.read_csv("jeux_donnees/parcoursup_2021.csv", delimiter=";")
# Ajout d'une colonne "Matiere" avec la valeur "Math" pour tous les élèves
parcoursup_2021_df["Annee"] = "2021"


# Ouverture du fichier csv Parcoursup 2020
parcoursup_2020_df = pd.read_csv("jeux_donnees/parcoursup_2020.csv", delimiter=";")
# Ajout d'une colonne "Matiere" avec la valeur "Math" pour tous les élèves
parcoursup_2020_df["Annee"] = "2020"


# Ouverture du fichier csv Parcoursup 2019
parcoursup_2019_df = pd.read_csv("jeux_donnees/parcoursup_2019.csv", delimiter=";")
# Ajout d'une colonne "Matiere" avec la valeur "Math" pour tous les élèves
parcoursup_2019_df["Annee"] = "2019"


# Ouverture du fichier csv Parcoursup 2018
parcoursup_2018_df = pd.read_csv("jeux_donnees/parcoursup_2018.csv", delimiter=";")
# Ajout d'une colonne "Matiere" avec la valeur "Math" pour tous les élèves
parcoursup_2018_df["Annee"] = "2018"


# Fusion des 5 dataframes en un seul
parcoursup_total_df = pd.concat([parcoursup_2022_df, parcoursup_2021_df, parcoursup_2020_df, parcoursup_2019_df, parcoursup_2018_df], ignore_index=True)

# parcoursup_total_df = pd.read_csv("donnees_traitees_finales.csv", delimiter=";")

df = parcoursup_total_df
#print(df.head())

# Cette partie du programme va permettre pour les colonnes contenant des strings de remplacer chaque différente valeur unique
#  par un numero de manière a pouvoir les utiliser dans des graphiques.
# Les associations nouveau numero attribué / ancien string sont ensuite faites dans des dictionnaires
# propres à chaque colonne, de manière à garder en mémoire à quel numero correspond quelle valeur de string.
detaillees_cols = []

#On fait une liste contenant les colonnes du dataframe contenant des strings à transformer en numéro
for col in df.columns :
    if df[col].dtype == "object" :
        detaillees_cols.append(col)

# On parcours toutes les colonnes du dataframe 
for col in df.columns:
    # On vérifie que la colonne contienne bien un string
    if df[col].dtype == "object" :
        # On récupère la liste de valeurs uniques pour chaque colonne.
        values = df[col].unique()
        # On déclare un dictionnaire.
        dico = {}
        # On ajoute la colonne aux colonnes detailles...
        detaillees_cols.append(col)
        # On va parcourir les différentes valeurs uniques. On dit que le premier numero remplaçant un string est 1
        for i, value in enumerate(values, start=1):
            # Le dictonnaire prend un nouveau couple clé / valeur
            dico[i] = value
            # toutes les colonnes possédant la valeur en question prennent le nouveau numero
            df[col] = df[col].replace(value, i)
        
        # On renomme le dictionnaire en mode " traduction_nomColonne"
        globals()["traduction_" + col] = dico





# # On défint le chemin vers les fichiers csv contenant les dictionnaires
# path = "dictionnaires_traduction/"

# # # On parcours tous les fichiers contenus dans le dossier 
# for filename in os.listdir(path) :
#     # On récupère pour chaque fichier le nom de la colonne du dataframe principal
#     # Dans ce sens d'abord on enlève l'extension du fichier ".csv"
#     col = filename[:-4]
#     # Ensuite on enlève le "traduction" présent en début des nom de fichiers
#     col = col.replace("traduction_", "") # retire le préfixe "traduction_"

#     # On sélectionne les fichiers .csv présents dans le dossier
#     if filename.endswith(".csv"):
#         # Par chaque fichier, on le transfère dans un dataframe
#         df = pd.read_csv(path + filename, delimiter=";")

#         # On récupère la liste de valeurs uniques pour la colonne principale 
#         values = df[col].unique()
#         # On déclare un dictionnaire
#         dico = {}
#         # On ajoute la colonne aux colonnes detailles...
#         detaillees_cols.append(col)

#         # On va parcourir les différentes valeurs uniques. On dit que le premier numero remplaçant un string est 1
#         for i, value in enumerate(values, start=1):
#             # Le dictonnaire prend un nouveau couple clé / valeur
#             dico[i] = value
#             # toutes les colonnes possédant la valeur en question prennent le nouveau numero
#             df[col] = df[col].replace(value, i)
#             # On renomme le dictionnaire en mode " traduction_nomColonne"

#         globals()["traduction_" + col] = dico


print()
print(traduction_dep_lib[11])


# print( "test traductions ...  :")
# print()
# print(traduction_cod_uai[1])
# print()
# print()

# # Affichage de toutes les colonnes et de leurs types de données
# print("Les colonnes et types de données du dataframe :")
# print(df.dtypes)
# print()
# print()
# print(" --------------------------------------------------- ")
# print()
# print()


# # Affichage des statistiques descriptives du dataframe
# print("Les statistiques descriptives du dataframe :")
# print(df.describe())
# print()
# print()
# print(" --------------------------------------------------- ")
# print()
# print()


# # Afficher le nombre de lignes et de colonnes dans le DataFrame
# print("Nombre total de lignes : ", df.shape[0])
# print("Nombre total de colonnes : ", df.shape[1])
# print()
# print()
# print(" --------------------------------------------------- ")
# print()
# print()

df1 = df.head(50)

#Exporter le dataframe en csv
print(" Exportation CSV en cours....")
df.to_csv('donnees_finales.csv', columns = df.columns,  index=False, sep=',')



# # Affichage de l'association entre le numéro et le string pour chaque colonne
# for col in df.columns:
#     # On vérifie que la colonne n'est pas binaire
#     if col in detaillees_cols :
#         print("Association des valeurs de la colonne", col, "avec leur numéro correspondant :")
#         # On affiche le dictionnaire correspondant à la colonne
#         print(globals()["traduction_" + col])
#         print()

# print(" --------------------------------------------------- ")
# print()
# print()



#Création du dossier si nécessaire
dossier = 'dictionnaires_traduction'
if not os.path.exists(dossier):
    os.mkdir(dossier)


# # Enregistrement des dictionnaires dans des fichiers CSV distincts
# for col in detaillees_cols:
#     # On récupère le nom du dictionnaire
#     nom_dico = 'traduction_' + col
#     # On récupère le dictionnaire correspondant
#     dico = globals()[nom_dico]
#     # On crée un DataFrame à partir du dictionnaire
#     df = pd.DataFrame(list(dico.items()), columns=['numero', col])

#     # On enregistre le DataFrame dans un fichier CSV portant le nom du dictionnaire
#     nom_fichier = os.path.join(dossier, nom_dico + '.csv')
#     df.to_csv(nom_fichier, index=False, sep=";")




