
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import psycopg2 as psy
from fonctions import *

'''
===================================================
                   CONSTANTES
===================================================
'''

tabNomFormation = ["Ecole d'Ingénieur", "Ecole de Commerce", "BTS", "Autre formation", "BUT", "CPGE", "Licence", "IFSI", "EFTS", "Licence_Las",  "PASS", "DUT", "PACES"]

# '''
# ===================================================
#                     Nom des tables
# ===================================================
# '''

# # typeFormation = "type" # type de formation
typeFormation = "fili"
pourcentAdmisFemme = "pct_f"
candidatTotal = "voe_tot"
# # candidatTotal = "effectif_candidats_total"
candidatFemme = "voe_tot_f"
# # candidatFemme = "effectif_candidats_femmes_total"
admisTotal = "acc_tot"
# # admisTotal = "effectifs_candidats_total_accepte"
admisFemme = "acc_tot_f"
# # admisFemme = "effectifs_candidats_femmes_total_accepte"

'''
===================================================
                        CSV
===================================================
'''

# df = pd.read_csv("csvSexe/Graphique1.csv", sep=",")
df = pd.read_csv("donnees_finales.csv", sep=",")
'''
===================================================
                  BASE DE DONNEES
===================================================
'''


# '''
# ===================================================
#                       ANALYSE
# ===================================================
# '''
# '''
# ---------------------------------------------------
#                 Sont-elle rejetée ?
# ---------------------------------------------------
# '''

# ### Récupération et préparation des données
tabFormation = [0 for i in range(13)] # Nombre de fois où le type de formation est apparu
tabPourcentageFemmeAdmises = [0 for i in range(13)] # Addition des pourcentages de femme (à diviser)

for i in range(len(df)):
    numFormation = (int) (df.at[i, typeFormation]-1) #Numéro de formation
    pourcent = df.at[i, pourcentAdmisFemme] # pourcentage de femme

    if (pourcent >= 0.0 and pourcent <= 100.0):
        tabFormation[numFormation] += 1
        tabPourcentageFemmeAdmises[numFormation] += pourcent

np.array(tabPourcentageFemmeAdmises)
np.array(tabFormation)

# Calcul sans tri
tabPourcentFemmes = [tabPourcentageFemmeAdmises[i]/tabFormation[i] for i in range(13)] #Moyenne des pourcentage par formation
tabPourcentHommes = [100-tabPourcentFemmes[i] for i in range(len(tabPourcentFemmes))]

# Tri
saveTabNomFormation = tabNomFormation
saveTabPourcentHommes = tabPourcentHommes
saveTabPourcentFemmes = tabPourcentFemmes

doubleTri_selection(tabPourcentFemmes, tabNomFormation)
tabPourcentHommes = [100-tabPourcentFemmes[i] for i in range(len(tabPourcentFemmes))]

### Affichage des données
plt.figure(figsize=(12.5, 6))
plt.barh(tabNomFormation, tabPourcentFemmes, label ="Femmes", color="#FEEFF3")
plt.barh(tabNomFormation, tabPourcentHommes, left=tabPourcentFemmes, label='Hommes', color="#A5CBEA")
plt.title("Pourcentage d'admissions par type de formation selon le sexe")
plt.xlabel('Pourcentage')

plt.legend()
plt.savefig('diagrams/sexe/png/pourcentAdmis.png', format='png')

plt.show()

#Reload
tabNomFormation = saveTabNomFormation
tabPourcentHommes = saveTabPourcentHommes 
tabPourcentFemmes = saveTabPourcentFemmes 

'''
---------------------------------------------------
                 Demande-t-elle ?
---------------------------------------------------
'''

# df = pd.read_csv("csvSexe/Graphique2.csv", sep=",")

### Récupération et préparation des données

tabDemandeTotal = [0 for i in range(13)]
tabDemandeFemme = [0 for i in range(13)]

tabFormation = [0 for i in range(13)] # Nombre de fois où le type de formation est apparu

for i in range(len(df)):
    numFormation = df.at[i, typeFormation]-1 #Numéro de formation
    demandeTotal = df.at[i, candidatTotal] # Nombre de voeux total
    demandeFemme = df.at[i, candidatFemme]

    tabDemandeFemme[numFormation] += demandeFemme
    tabDemandeTotal[numFormation] += demandeTotal
    tabFormation[numFormation] += 1

tabDemandeTotalMoy = [tabDemandeTotal[i]/tabFormation[i] for i in range(13)] # Moyenne des demandes par formation totale
tabDemandeFemmeMoy = [tabDemandeFemme[i]/tabFormation[i] for i in range(13)] # Moyenne des demandes par formation des femmes

# Calcul sans tri
tabDemandePourcentageFemme = [tabDemandeFemmeMoy[i]/tabDemandeTotalMoy[i]*100 for i in range(13)]
tabDemandePourcentageHomme = [100-tabDemandePourcentageFemme[i] for i in range(13)]

#Tri
saveTabNomFormation = tabNomFormation
savetabDemandePourcentageHomme = tabDemandePourcentageHomme
saveDemandePourcentageFemme = tabDemandePourcentageFemme

doubleTri_selection(tabDemandePourcentageFemme, tabNomFormation)
tabDemandePourcentageHomme = [100-tabDemandePourcentageFemme[i] for i in range(len(tabDemandePourcentageFemme))]
# ### Affichage des données

plt.figure(figsize=(12.5, 6))
plt.barh(tabNomFormation, tabDemandePourcentageFemme, label ="Femmes", color="#FEEFF3")
plt.barh(tabNomFormation, tabDemandePourcentageHomme, left=tabDemandePourcentageFemme, label='Hommes', color="#A5CBEA")

plt.title("Pourcentage de voeux par type de formation selon le sexe")
plt.xlabel('Pourcentage')

plt.legend()
plt.savefig('diagrams/sexe/png/pourcentVoeux.png', format='png')

plt.show()

# Reload
tabNomFormation = saveTabNomFormation
tabDemandePourcentageHomme = savetabDemandePourcentageHomme
tabDemandePourcentageFemme = saveDemandePourcentageFemme

# '''
# ----------------------------------------------------
#           Y-a-t'il vraiment des inégalités
# ----------------------------------------------------
# '''
# # df = pd.read_csv("csvSexe/Graphique3.csv", sep=",")

### Récupération et préparation des données
numFormations = [0 for i in range(13)]
tabFormation = [0 for i in range(13)] # Nombre de fois où le type de formation est apparu
tabNbAdmissionTotal = [0 for i in range(13)]
tabNbAdmissionFemme = [0 for i in range(13)]

tabDemandeHommeMoy = [tabDemandeTotalMoy[i]-tabDemandeFemmeMoy[i] for i in range(13)]

for i in range(len(df)):
    numFormation = df.at[i, typeFormation]-1 #Numéro de formation 
    nbAdmisTotal = df.at[i, admisTotal]
    nbAdmisFemme = df.at[i, admisFemme]

    tabFormation[numFormation] += 1
    
    tabNbAdmissionFemme[numFormation] += nbAdmisFemme
    tabNbAdmissionTotal[numFormation] += nbAdmisTotal

tabNbAdmissionHomme = [tabNbAdmissionTotal[i]-tabNbAdmissionFemme[i] for i in range(13)] # Nombre d'admissions des hommes additionné par formation

tabNbAdmissionHommeMoy = [tabNbAdmissionHomme[i]/tabFormation[i] for i in range(13)] # Moyenne des admissions par formation des hommes
tabNbAdmissionFemmeMoy = [tabNbAdmissionFemme[i]/tabFormation[i] for i in range(13)] # Moyenne des admissions par formation des femmes

# # Calcul du ration admission/voeux par sexe

tabRatioFemme = [tabNbAdmissionFemmeMoy[i]/tabDemandeFemmeMoy[i]*100 for i in range(13)]
tabRatioHomme = [tabNbAdmissionHommeMoy[i]/tabDemandeHommeMoy[i]*100 for i in range(13)]
print(tabRatioFemme)
print(tabRatioHomme)

### Affichage des données

fig, ax = plt.subplots()

bar_width = 0.35
index = np.arange(len(tabNomFormation))

bar1 = ax.bar(index, tabRatioFemme, bar_width, label='Femmes', color='#FFD9DF')
bar2 = ax.bar(index + 0.42, tabRatioHomme, bar_width, label='Hommes', color='#A5CBEA')

# Configuration des axes et de la légende
ax.set_xlabel('Formations')
ax.set_ylabel("Pourcentage d'admis")
ax.set_title("Pourcentage d'admissions mis en relation au proportion de voeux")
ax.set_xticks(index + bar_width / 2)
ax.set_xticklabels(tabNomFormation, rotation=90)
ax.legend()

plt.tight_layout()

plt.savefig('diagrams/sexe/png/ratioAdmissionDemande.png', format='png')
plt.show()