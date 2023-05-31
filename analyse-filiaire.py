import os
import pandas as pd
import numpy as np
import psycopg2 as psy
import matplotlib.pyplot as plt

# Chargement dico 

tab = ['form_lib_voe_acc']

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
        df_candidats = pd.read_csv(path + filename, delimiter=";")

        # On récupère la liste de valeurs uniques pour la colonne principale 
        values = df_candidats[col].unique()
        # On déclare un dictionnaire
        dico = {}
        # On ajoute la colonne aux colonnes detailles...
        #detaillees_cols.append(col)
        
        # On va parcourir les différentes valeurs uniques. On dit que le premier numero remplaçant un string est 1
        for i, value in enumerate(values, start=1):
            # Le dictonnaire prend un nouveau couple clé / valeur
            dico[i] = value
            # toutes les colonnes possédant la valeur en question prennent le nouveau numero
            df_candidats[col] = df_candidats[col].replace(value, i)
            # On renomme le dictionnaire en mode " traduction_nomColonne"

        globals()["traduction_" + col] = dico
    
# Fin chargement dico


try :
    co = psy.connect(host = 'londres',
                    database = 'dbjemarcilla',
                    user = 'jemarcilla',
                    password = "achanger")
    df_candidats = pd.read_sql('''SELECT F.nom,
                                    sum(S.effectifs_candidats_term_general_principale + effectifs_candidats_term_generale_complementaire)  AS Nb_Candidats_Bac_Generale,
                                    sum(effectifs_candidats_neo_term_tech_principale + effectifs_candidats_term_tech_complementaire) AS nb_candidats_bac_technologique,
                                    sum(effectifs_candidats_neo_term_pro_principale + effectifs_candidats_term_pro_complementaire) AS nb_candidats_bac_pro,
                                    sum(effectif_candidats_total - effectifs_candidats_autres_principale  - effectifs_candidats_autres_complementaire)  AS nb_total
                                    FROM Statistiques S, Formation F
                                    WHERE S.formation = F.nom
                                    GROUP BY F.nom
                                    ORDER by 1
                            ;''', con = co)
    df_propositions = pd.read_sql('''SELECT F.nom,
                                    sum(effectifs_candidats_proposition_term_generale)  AS nb_propositions_bac_generale,
                                    sum(effectifs_candidats_proposition_term_tech ) AS nb_propositions_bac_technologique,
                                    sum(effectifs_candidats_proposition_term_pro ) AS nb_propositions_bac_pro,
                                    sum(effectifs_candidats_total_proposition)  AS nb_total
                                    FROM Statistiques S, Formation F
                                    WHERE S.formation = F.nom
                                    AND effectifs_candidats_proposition_term_generale != 'NaN'
                                    GROUP BY F.nom
                                    ORDER by 1
                            ;''', con = co)
    df_mention = pd.read_sql('''SELECT F.nom,
                                    AVG(S.pct_sansmention) AS sans_mention,
                                    AVG(S.pct_AB) AS mention_AB,
                                    AVG(pct_B) AS mention_B,
                                    AVG(pct_TB) AS mention_TB,
                                    AVG(pct_TBF) AS mention_TBF
                                    FROM Statistiques S, Formation F
                                    WHERE S.formation = F.nom
                                    AND pct_AB != 'NaN'
                                    AND pct_TBF != 'NaN'
                                    GROUP BY F.nom
                                    ORDER by 1
                            ;''', con = co)

    # Taux de proposition accepté par candidats d'une filiaire
    df_taux = pd.read_sql('''SELECT F.nom,
                                    SUM(effectifs_candidats_proposition_term_generale)  / NULLIF(SUM(effectifs_candidats_term_general_principale + effectifs_candidats_term_generale_complementaire), 0) AS taux_reussite_generale,
                                    SUM(effectifs_candidats_proposition_term_tech)  / NULLIF(SUM(effectifs_candidats_neo_term_tech_principale + effectifs_candidats_term_tech_complementaire), 0) AS taux_reussite_tech,
                                    SUM(effectifs_candidats_proposition_term_pro) / NULLIF(SUM(effectifs_candidats_neo_term_pro_principale + effectifs_candidats_term_pro_complementaire), 0) AS taux_reussite_pro
                                    FROM Statistiques S, Formation F
                                    WHERE S.formation = F.nom
                                    AND effectifs_candidats_proposition_term_generale != 'NaN'
                                    AND effectifs_candidats_proposition_term_tech != 'NaN' 
                                    AND effectifs_candidats_proposition_term_pro != 'NaN'
                                    AND effectifs_candidats_term_general_principale != 'NaN' 
                                    AND effectifs_candidats_neo_term_tech_principale != 'NaN' 
                                    AND effectifs_candidats_neo_term_pro_principale != 'NaN'
                                    AND effectifs_candidats_term_generale_complementaire != 'NaN'
                                    AND effectifs_candidats_term_tech_complementaire != 'NaN'
                                    AND effectifs_candidats_term_pro_complementaire != 'NaN'
                                    GROUP BY F.nom
                                    ORDER by 1 
                            ;''', con = co)
    print(df_taux)
    # Creation du dataframe candidats
    df_candidats = pd.DataFrame(df_candidats)
    df_candidats = df_candidats.loc[[0, 2, 6, 8,11, 14]]
    noms_candidats = df_candidats['nom'].to_list()

    # Creation du dataframe proposition
    df_propositions = pd.DataFrame(df_propositions)
    df_propositions = df_propositions.loc[[0, 2, 6, 8,11, 14]]
    noms_propositions = df_propositions['nom'].to_list()

    # Creation du dataframe mention
    df_mention = pd.DataFrame(df_mention)
    df_mention = df_mention.loc[[0, 2, 6, 8,11, 14]]
    noms_propositions = df_propositions['nom'].to_list()

  # Creation du dataframe taux
    df_taux = pd.DataFrame(df_taux)
    df_taux = df_taux.loc[[0, 2, 6, 8,11, 14]]




    # Valeurs des barres pour chaque nom
    #   Pour les candidats
    valeurs_generale = df_candidats['nb_candidats_bac_generale']  
    valeurs_technologique = df_candidats['nb_candidats_bac_technologique']
    valeurs_pro = df_candidats['nb_candidats_bac_pro']
    #   Pour ceux qui ont eu une proposition
    valeurs_generale_pr = df_propositions['nb_propositions_bac_generale']  
    valeurs_technologique_pr = df_propositions['nb_propositions_bac_technologique']
    valeurs_pro_pr = df_propositions['nb_propositions_bac_pro']

    #Pour les mentions
    valeurs_sansmention = df_mention['sans_mention']
    valeurs_mention_Assez_Bien = df_mention['mention_ab']
    valeurs_mention_Bien = df_mention['mention_b']
    valeurs_mention_Tres_Bien = df_mention['mention_tb']
    valeurs_mention_Tres_Bien_Felicitation = df_mention['mention_tbf']

    #Pour les taux de reussites
    valeurs_taux_generale = df_taux['taux_reussite_generale']
    valeurs_taux_tech = df_taux['taux_reussite_tech']
    valeurs_taux_pro = df_taux['taux_reussite_pro']

    # Normalisation des valeurs
    valeurs_generale_normalized = valeurs_generale / (valeurs_pro + valeurs_technologique + valeurs_generale)
    valeurs_technologique_normalized = valeurs_technologique / (valeurs_pro + valeurs_technologique + valeurs_generale)
    valeurs_pro_normalized = valeurs_pro / (valeurs_pro + valeurs_technologique + valeurs_generale)

    #   Pour ceux qui ont eu une proposition
    valeurs_generale_pr_normalized = valeurs_generale_pr / (valeurs_pro_pr + valeurs_technologique_pr + valeurs_generale_pr)
    valeurs_technologique_pr_normalized = valeurs_technologique_pr / (valeurs_pro_pr + valeurs_technologique_pr + valeurs_generale_pr)
    valeurs_pro_pr_normalized = valeurs_pro_pr / (valeurs_pro_pr + valeurs_technologique_pr + valeurs_generale_pr)


    # Couleurs des barres
    couleurs = ['blue', 'green', 'orange', 'cyan']

    # Position des barres sur l'axe des abscisses
    position = np.arange(len(noms_candidats))

    # Largeur des barres
    largeur = 0.20

    # Création du graphique candidats
    plt.figure()
    plt.bar(position, valeurs_generale_normalized, width=largeur, color=couleurs[0], label='Bac général')
    plt.bar(position + largeur, valeurs_technologique_normalized, width=largeur, color=couleurs[1], label='Bac technologique')
    plt.bar(position + 2*largeur, valeurs_pro_normalized, width=largeur, color=couleurs[2], label='Bac professionnel')

    # Modifier les étiquettes de l'axe des abscisses avec les noms
    for i in range(len(noms_candidats)):
        noms_candidats[i] = traduction_form_lib_voe_acc[noms_candidats[i]]
    plt.xticks(position + largeur, noms_candidats)

    # Ajouter une légende
    plt.legend()
    plt.gca().set_yticklabels(['{:.0f}%'.format(x*100) for x in plt.gca().get_yticks()])
    # Titre du graphique
    plt.title('Taux de candidats selon le type de baccalauréats par formation')

    ##Creation du graphique propositions8
    plt.figure()
    plt.bar(position, valeurs_generale_pr_normalized, width=largeur, color=couleurs[0], label='Bac général')
    plt.bar(position + largeur, valeurs_technologique_pr_normalized, width=largeur, color=couleurs[1], label='Bac technologique')
    plt.bar(position + 2*largeur, valeurs_pro_pr_normalized, width=largeur, color=couleurs[2], label='Bac professionnel')

    # Modifier les étiquettes de l'axe des abscisses avec les noms
    for i in range(len(noms_propositions)):
        noms_propositions[i] = traduction_form_lib_voe_acc[noms_propositions[i]]
    plt.xticks(position + largeur, noms_propositions)
#
    # Ajouter une légende
    plt.legend()
    plt.gca().set_yticklabels(['{:.0f}%'.format(x*100) for x in plt.gca().get_yticks()])
    ## Titre du graphique
    plt.title('Taux de propositions selon le type baccalauréats par formation')
#
    ## Afficher les graphiques
    plt.show()

    ##Creation du graphique propositions
    plt.figure()
    plt.bar(position, valeurs_sansmention, width=largeur, color=couleurs[0], label='Sans mention')
    plt.bar(position + largeur, valeurs_mention_Assez_Bien, width=largeur, color=couleurs[1], label='Mention assez bien')
    plt.bar(position + 2*largeur, valeurs_mention_Bien, width=largeur, color=couleurs[2], label='Mention bien')
    plt.bar(position + 3*largeur, valeurs_mention_Tres_Bien + valeurs_mention_Tres_Bien_Felicitation, width=largeur, color=couleurs[3], label='Mention tres bien')

    #Modifier les étiquettes de l'axe des abscisses avec les noms
    plt.xticks(position + largeur, noms_propositions)

    # Ajouter une légende
    plt.legend()
    ## Titre du graphique
    plt.title('Taux de mentions par candidats admis')

    # Afficher le graphiques
    plt.show()

    plt.figure()
    plt.bar(position, valeurs_taux_generale, width=largeur, color=couleurs[0], label='Taux bac generale')
    plt.bar(position + largeur, valeurs_taux_tech, width=largeur, color=couleurs[1], label='Taux bac technologique')
    plt.bar(position + 2*largeur, valeurs_taux_pro, width=largeur, color=couleurs[2], label='Taux bac pro')

    #Modifier les étiquettes de l'axe des abscisses avec les noms
    plt.xticks(position + largeur, noms_propositions)

    # Ajouter une légende
    plt.legend()
    ## Titre du graphique
    plt.title("Taux de proposition pour une candidature donnée selon la filiaire au bac")

    # Afficher le graphiques
    plt.show()

except( Exception, psy.DatabaseError ) as error :
    print( error )

finally :
    if co is not None :
        co.close()