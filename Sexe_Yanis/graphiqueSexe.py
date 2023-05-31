import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from fonctions import *

'''
===================================================
                   CONSTANTES
===================================================
'''

tabNomFormation = ["Ecole d'Ingénieur", "Ecole de Commerce", "BTS", "Autre formation", "BUT", "CPGE", "Licence", "IFSI", "EFTS", "Licence_Las",  "PASS", "DUT", "PACES"]


'''
===================================================
                        CSV
===================================================
'''
typeFormation = "fili" # A commenter pour utiliser le csv de la requête
pourcentAdmisFemme = "pct_f" # A commenter pour utiliser le csv de la requête
candidatTotal = "voe_tot" # A commenter pour utiliser le csv de la requête
candidatFemme = "voe_tot_f" # A commenter pour utiliser le csv de la requête
admisTotal = "acc_tot" # A commenter pour utiliser le csv de la requête
admisFemme = "acc_tot_f" # A commenter pour utiliser le csv de la requête

df = pd.read_csv("../donnees_finales.csv", sep=",") # A commenter pour utiliser le csv de la requête

'''
===================================================
                  BASE DE DONNEES
===================================================
'''
# typeFormation = "type" # type de formation # A décommenter pour utiliser le csv de la requête
# admisFemme = "effectifs_candidats_femmes_total_accepte" # A décommenter pour utiliser le csv de la requête
# admisTotal = "effectifs_candidats_total_accepte" # A décommenter pour utiliser le csv de la requête
# candidatTotal = "effectif_candidats_total" # A décommenter pour utiliser le csv de la requête
#  candidatFemme = "effectif_candidats_femmes_total" # A décommenter pour utiliser le csv de la requête

# df = pd.read_csv("csvSexe/Graphique1.csv", sep=",") # A décommenter pour utiliser le csv de la requête

'''
===================================================
                      ANALYSE
===================================================
'''
'''
---------------------------------------------------
                Sont-elle rejetée ?
---------------------------------------------------
'''

### Récupération et préparation des données
tabFormation = [0 for i in range(13)] # Nombre de fois où le type de formation est apparu
tabPourcentageFemmeAdmises = [0 for i in range(13)] # Addition des pourcentages de femme (à diviser)

for i in range(len(df)):
    numFormation = (int) (df.at[i, typeFormation]-1) # Numéro de formation
    pourcent = df.at[i, pourcentAdmisFemme] # Pourcentage de femme

    if (pourcent >= 0.0 and pourcent <= 100.0):
        tabFormation[numFormation] += 1
        tabPourcentageFemmeAdmises[numFormation] += pourcent

### Calcul sans tri
tabPourcentFemmes = [tabPourcentageFemmeAdmises[i]/tabFormation[i] for i in range(13)] # Moyenne des pourcentage par formation
tabPourcentHommes = [100-tabPourcentFemmes[i] for i in range(len(tabPourcentFemmes))]

### Tri
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
plt.savefig('diagrams/png/pourcentAdmis.png', format='png')

plt.show()

### Reload
tabNomFormation = saveTabNomFormation
tabPourcentHommes = saveTabPourcentHommes 
tabPourcentFemmes = saveTabPourcentFemmes 

'''
---------------------------------------------------
                 Demande-t-elle ?
---------------------------------------------------
'''

# df = pd.read_csv("csvSexe/Graphique2.csv", sep=",") # A décommenter pour utiliser le csv de la requête

### Récupération et préparation des données

tabDemandeTotal = [0 for i in range(13)]
tabDemandeFemme = [0 for i in range(13)]

tabFormation = [0 for i in range(13)] # Nombre de fois où le type de formation est apparu

for i in range(len(df)):
    numFormation = df.at[i, typeFormation]-1 # Numéro de formation
    demandeTotal = df.at[i, candidatTotal] # Nombre de voeux total
    demandeFemme = df.at[i, candidatFemme]

    tabDemandeFemme[numFormation] += demandeFemme
    tabDemandeTotal[numFormation] += demandeTotal
    tabFormation[numFormation] += 1

tabDemandeTotalMoy = [tabDemandeTotal[i]/tabFormation[i] for i in range(13)] # Moyenne des demandes par formation totale
tabDemandeFemmeMoy = [tabDemandeFemme[i]/tabFormation[i] for i in range(13)] # Moyenne des demandes par formation des femmes

### Calcul sans tri
tabDemandePourcentageFemme = [tabDemandeFemmeMoy[i]/tabDemandeTotalMoy[i]*100 for i in range(13)]
tabDemandePourcentageHomme = [100-tabDemandePourcentageFemme[i] for i in range(13)]

### Tri
saveTabNomFormation = tabNomFormation
savetabDemandePourcentageHomme = tabDemandePourcentageHomme
saveDemandePourcentageFemme = tabDemandePourcentageFemme

doubleTri_selection(tabDemandePourcentageFemme, tabNomFormation)
tabDemandePourcentageHomme = [100-tabDemandePourcentageFemme[i] for i in range(len(tabDemandePourcentageFemme))]

### Affichage des données

plt.figure(figsize=(12.5, 6))
plt.barh(tabNomFormation, tabDemandePourcentageFemme, label ="Femmes", color="#FEEFF3")
plt.barh(tabNomFormation, tabDemandePourcentageHomme, left=tabDemandePourcentageFemme, label='Hommes', color="#A5CBEA")

plt.title("Pourcentage de voeux par type de formation selon le sexe")
plt.xlabel('Pourcentage')

plt.legend()
plt.savefig('diagrams/png/pourcentVoeux.png', format='png')

plt.show()

### Reload
tabNomFormation = saveTabNomFormation
tabDemandePourcentageHomme = savetabDemandePourcentageHomme
tabDemandePourcentageFemme = saveDemandePourcentageFemme

'''
----------------------------------------------------
          Y-a-t'il vraiment des inégalités
----------------------------------------------------
'''
# df = pd.read_csv("csvSexe/Graphique3.csv", sep=",") # A décommenter pour utiliser le csv de la requête

### Récupération et préparation des données
numFormations = [0 for i in range(13)]
tabFormation = [0 for i in range(13)] # Nombre de fois où le type de formation est apparu
tabNbAdmissionTotal = [0 for i in range(13)]
tabNbAdmissionFemme = [0 for i in range(13)]

tabDemandeHommeMoy = [tabDemandeTotalMoy[i]-tabDemandeFemmeMoy[i] for i in range(13)]

for i in range(len(df)):
    numFormation = df.at[i, typeFormation]-1 # Numéro de formation 
    nbAdmisTotal = df.at[i, admisTotal]
    nbAdmisFemme = df.at[i, admisFemme]

    tabFormation[numFormation] += 1
    
    tabNbAdmissionFemme[numFormation] += nbAdmisFemme
    tabNbAdmissionTotal[numFormation] += nbAdmisTotal

tabNbAdmissionHomme = [tabNbAdmissionTotal[i]-tabNbAdmissionFemme[i] for i in range(13)] # Nombre d'admissions des hommes additionné par formation

tabNbAdmissionHommeMoy = [tabNbAdmissionHomme[i]/tabFormation[i] for i in range(13)] # Moyenne des admissions par formation des hommes
tabNbAdmissionFemmeMoy = [tabNbAdmissionFemme[i]/tabFormation[i] for i in range(13)] # Moyenne des admissions par formation des femmes

### Calcul du ration admission/voeux par sexe

tabRatioFemme = [tabNbAdmissionFemmeMoy[i]/tabDemandeFemmeMoy[i]*100 for i in range(13)]
tabRatioHomme = [tabNbAdmissionHommeMoy[i]/tabDemandeHommeMoy[i]*100 for i in range(13)]

### Affichage des données

fig, ax = plt.subplots()

bar_width = 0.35
index = np.arange(len(tabNomFormation))

bar1 = ax.bar(index, tabRatioFemme, bar_width, label='Femmes', color='#FFD9DF')
bar2 = ax.bar(index + 0.42, tabRatioHomme, bar_width, label='Hommes', color='#A5CBEA')

ax.set_xlabel('Formations')
ax.set_ylabel("Pourcentage d'admis")
ax.set_title("Pourcentage d'admissions mis en relation au proportion de voeux")
ax.set_xticks(index + bar_width / 2)
ax.set_xticklabels(tabNomFormation, rotation=90)
ax.legend()

plt.tight_layout()

plt.savefig('diagrams/png/ratioAdmissionDemande.png', format='png')
plt.show()