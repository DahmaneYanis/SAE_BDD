import pandas as pd
#import psycopg2 as psy
import os

data = pd.read_csv(r'donnees_finales.csv', sep=',')
df1 = pd.DataFrame(data)

df = df1

tab = ['fili']

print()
print()
# # On définit le chemin vers les fichiers csv contenant les dictionnaires
path = "dictionnaires_traduction/"

# # On parcours tous les fichiers contenus dans le dossier 
for filename in os.listdir(path) :
    # On récupère pour chaque fichier le nom de la colonne du dataframe principal
    # Dans ce sens d'abord on enlève l'extension du fichier ".csv"
    col = filename[:-4]

    # Ensuite on enlève le "traduction" présent en début des nom de fichiers
    col = col.replace("traduction_", "") # retire le préfixe "traduction_"

    # On sélectionne les fichiers .csv présents dans le dossier
    if filename.endswith(".csv") and col in tab :

        print( " -> Dictionnaire " + col + " en cours de chargement en python ")

        # Par chaque fichier, on le transfère dans un dataframe
        df = pd.read_csv(path + filename, delimiter=";")

        # On récupère la liste de valeurs uniques pour la colonne principale 
        values = df[col].unique()
        # On déclare un dictionnaire
        dico = {}
        # On ajoute la colonne aux colonnes detailles...
        #detaillees_cols.append(col)
        
        # On va parcourir les différentes valeurs uniques. On dit que le premier numero remplaçant un string est 1
        for i, value in enumerate(values, start=1):
            # Le dictonnaire prend un nouveau couple clé / valeur
            dico[i] = value
            # toutes les colonnes possédant la valeur en question prennent le nouveau numero
            df[col] = df[col].replace(value, i)
            # On renomme le dictionnaire en mode " traduction_nomColonne"

        globals()["traduction_" + col] = dico
    
    


print()
print(" ------------------------------- ")
print()
print()
print(traduction_fili)
print()
print()