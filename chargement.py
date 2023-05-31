import pandas as pd
import psycopg2 as psy

# On récupère les données qui ont été traitées au préalable 
data = pd.read_csv(r'donnees_finales.csv', sep=',')

# On renomme le dataframe utilisé 
df = data


# ======================
# Ouverture du curseur :
# ======================

try :
    print("Connexion à Londres . . .")
    co = psy.connect(host = 'londres',
                    database = 'dbjemarcilla',
                    user = 'jemarcilla',
                    password = "achanger")
    print("Connecté à Londres !")

    curs = co.cursor()



# ================================
# Création de la base de données : 
# ================================

    # On commence par supprimer toutes les tables déjà existantes en tenant 
    # bien compte des dépendances entre les tables 
    curs.execute('''DROP TABLE IF EXISTS Formation;''')
    curs.execute('''DROP TABLE IF EXISTS Etablissement;''')
    curs.execute('''DROP TABLE IF EXISTS departement;''')
    curs.execute('''DROP TABLE IF EXISTS Region;''')

    # Tout au long du fichier des messages informatifs comme celui-ci permettent
    # de tenir informé l'utilisateur de l'avancée du programme
    print(" -> Tous les DROP TABLE réalisés")



#Table établissement :
#- - - - - - - - - - - 

    # On créé la table 
    curs.execute('''
                    CREATE TABLE Etablissement
                    (
                        code numeric(10) PRIMARY KEY,
                        geo_localisation numeric(10),
                        departement numeric(3) REFERENCES Departement
                    );
                    ''')
    # On informe que la table est créée 
    print()
    print(" -> Table Etablissement créée ")

    # On charge les données dans la table 
    for row in df.itertuples() :
                curs.execute( '''INSERT INTO Etablissement VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;''',
                (row.cod_uai, row.g_ea_lib_vx, row.dep))

    # On informe de la fin de chargement des données 
    print(" -> Chargement des données dans Etablissement réalisé ")




# Table région : 
#  - - - - - - - -

    # On créé la table Region 
    curs.execute('''
                     CREATE TABLE Region
                     (
                        nom numeric(10) PRIMARY KEY,
                        academie numeric(10)
                        
                     );
                 ''')
    # On informe que la table a été créée 
    print()
    print(" -> Table Region créée")

    # On charge les donnnées dans la table 
    for row in df.itertuples() :
        curs.execute( '''INSERT INTO Region VALUES (%s, %s) ON CONFLICT DO NOTHING;''', (row.region_etab_aff, row.acad_mies,))
    
    # On informe que les données ont bien été chargées
    print(" -> Chargement des données dans Region réalisé")




# Table Departement : 
# - - - - - - - - - - 

    # On créé la table Departement 
    curs.execute('''
                    CREATE TABLE Departement
                    (
                        numero numeric(3) PRIMARY KEY,
                        nom numeric(10),
                        region numeric(10) REFERENCES Region
                    );
                ''')
    # On informe que la table a été créée 
    print()
    print(" -> Table departement créée ")

    # On charge les données dans la table 
    for row in df.itertuples() :
                curs.execute( '''INSERT INTO Departement VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;''',
                    (row.dep, row.dep_lib, row.region_etab_aff,))
                
    # On informe que la table a bien été créée 
    print(" -> Chargement des données dans Departement réalisé ")
               



# Table Formation :
#- - - - - - - - - - 

    # On créé la table formation
    curs.execute('''
                    CREATE TABLE Formation
                    (
                        nom numeric(10) PRIMARY KEY,
                        contrat numeric(4),
                        selectif numeric(4)
                    );
               ''')
    # On informe que la table a été créée 
    print()
    print(" -> Table Formation créée ")
    
    # On charge les données dans la table 
    for row in df.itertuples() :
                curs.execute( '''INSERT INTO Formation VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;''',
                    (row.form_lib_voe_acc, row.contrat_etab, row.select_form))
                
    # On informe que les données ont bien été chargées
    print(" -> Chargement des données dans Formation réalisé ")



# Table Statistiques :
#- - - - - - - - - - - 

    # On créé la table Statistiques 
    curs.execute('''DROP TABLE IF EXISTS Statistiques;''')
    curs.execute('''
                    CREATE TABLE Statistiques
                    (
                        session numeric(10),
                        etablissement numeric(10) REFERENCES Etablissement,
                        formation numeric(10) REFERENCES Formation,

                        capa_fin numeric(10), 

                        effectif_candidats_total numeric(10), -- 1 ; voe_tot
                        effectif_candidats_femmes_total numeric(10), -- 2 ; voe_tot_f
                        effectif_candidats_total_principale numeric(10), -- 3 ; nb_VOE_PP
                        effectifs_candidats_term_general_principale numeric(10), -- 4 ; nb_VOE_PP_BG
                        effectifs_candidats_term_generale_boursier_principale numeric(10), -- 5 ; nb_VOE_PP_BG_brs
                        effectifs_candidats_neo_term_tech_principale numeric(10), -- 6 ; nb_VOE_PP_BT
                        effectifs_candidats_boursiers_term_tech_principale numeric(10), -- 7 ; nb_VOE_PP_BT _brs
                        effectifs_candidats_neo_term_pro_principale numeric(10), -- 8 ; nb_VOE_PP_BP
                        effectifs_candidats_boursiers_neo_term_pro_principale numeric(10), -- 9 ; nb_VOE_PP_BP_brs
                        effectifs_candidats_autres_principale numeric(10), -- 10 ; nb_VOE_PP_at
                        effectifs_candidats_total_complementaire numeric(10), -- 11 ; nb_VOE_PC
                        effectifs_candidats_term_generale_complementaire numeric(10), -- 12; nb_VOE_PC_BG
                        effectifs_candidats_term_tech_complementaire numeric(10), -- 13 ; nb_VOE_PC_BT
                        effectifs_candidats_term_pro_complementaire numeric(10), -- 14 ; nb_VOE_PC_BP
                        effectifs_candidats_autres_complementaire numeric(10), -- 15 ; nb_VOE_PC_at

                        effectifs_candidats_total_classes_principale numeric(10), -- 16 ; nb_cla_PP
                        effectifs_candidats_total_classes_complementaire numeric(10), -- 17 ; nb_cla_PC
                        effectifs_candidats_classes_term_generale numeric(10), -- 18 ; nb_cla_pp_BG
                        effectifs_candidats_classes_boursiers_term_generale numeric(10), -- 19 ; nb_cla_pp_BG_brs
                        effectifs_candidats_classes_term_tech numeric(10), -- 20 ; nb_cla_pp_bt
                        effectifs_candidats_classes_boursiers_term_tech numeric(10), -- 21 ; nb_cla_pp_bt_brs
                        effectifs_candidats_classes_term_pro numeric(10), -- 22 ; nb_cla_pp_BP
                        effectifs_candidats_classes_boursiers_term_pro numeric(10), -- 23 ; nb_cla_pp_bp_brs
                        effectifs_candidats_classes_autres numeric(10), -- 24 ; nb_cla_pp_at -- 

                        effectifs_candidats_total_proposition numeric(10), -- 25; Prop_tot
                        effectifs_candidats_proposition_term_generale numeric(10), -- 26 ; prop_tot_BG
                        effectifs_candidats_boursiers_proposition_term_generale numeric(10), -- 27 ; prop_tot_bg_brs
                        effectifs_candidats_proposition_term_tech numeric(10), -- 28 ; prop_tot_bt
                        effectifs_candidats_boursiers_proposition_term_tech numeric(10), -- 29 ; prop_tot_bt_brs
                        effectifs_candidats_proposition_term_pro numeric(10), -- 30 ; prop_tot_bp
                        effectifs_candidats_boursiers_proposition_term_pro numeric(10), -- 31 ; prop_tot_bp_brs
                        effectifs_candidats_autres_proposition numeric(10), -- 32 ; prop_tot_at 

                        effectifs_candidats_total_accepte numeric(10), -- 33 ; acc_tot ( Hommes + femmes)
                        effectifs_candidats_femmes_total_accepte numeric(10), -- 34  ; acc_tot_f ( Que femmes)
                        effectifs_candidats_admis_principale numeric(10), -- 35 ; acc_pp
                        effectifs_candidats_admis_complementaire numeric(10), -- 36 ; acc_pc
                        effectifs_candidats_boursiers_neo_admis_complementaire numeric(10), -- 37 ; acc_brs
                        effectifs_candidats_admis_avant_fin_procedure_principale numeric(10), -- 38 ; acc_finpp

                        effectifs_candidats_total_neo_admis numeric(10), -- 39 ; acc_neobac
                        effectifs_candidats_neo_generaux_admis numeric(10), -- 40 ; acc_bg
                        effectifs_candidats_neo_term_tech_admis numeric(10), -- 41 ; acc_bt
                        effectifs_candidats_neo_term_pro_admis numeric(10), -- 42 ; acc_bp
                        effectifs_candidats_autres_admis numeric(10), -- 43 ; acc_at

                        effectifs_candidats_mt_non_renseignee numeric(10), -- 44 ; acc_mention_nonrenseignee
                        effectifs_candidats_sans_mt numeric(10), -- 45 ; acc_sansmention
                        effectifs_candidats_mt_ab numeric(10), -- 46 ; acc_ab
                        effectifs_candidats_mt_b numeric(10), -- 47 ; acc_b
                        effectifs_candidats_mt_tb numeric(10), -- 48 ; acc_tb
                        effectifs_candidats_mt_tbf numeric(10), -- 49 ; acc_tbf

                        pct_f numeric(10), -- 50
                        pct_aca_orig numeric(10), -- 51
                        pct_bours numeric(10), -- 52 
                        pct_acc_debutpp numeric(10), -- 53
                        pct_acc_finpp numeric(10), -- 54
                        pct_neobac numeric(10), -- 55
                        pct_mention_nonrenseignee numeric(10), -- 56
                        pct_sansmention numeric(10), -- 57
                        pct_AB numeric(10), -- 58
                        pct_B  numeric(10), -- 59
                        pct_TB numeric(10), -- 60
                        pct_TBF numeric(10), -- 61
                        pct_bg numeric(10), --62 
                        pct_bg_mention numeric(10), -- 63 
                        pct_bt numeric(10), -- 64
                        pct_bt_mention numeric(10), -- 65 
                        pct_bp numeric(10), -- 66
                        pct_bp_mention numeric(10), -- 67 
                        taux_acces_ens numeric(10), -- 68 
                        part_acces_gen numeric(10), -- 69
                        part_acces_tec numeric(10), -- 70
                        part_acces_pro numeric(10), -- 71 
                        cod_aff_form numeric(10), -- 72 


                        PRIMARY KEY (etablissement, formation, session)
                    );
                ''')
    # On informe que la table a bien été créée 
    print()
    print(" -> Table Statistiques créée ")

    # On charge les données dans la table 
    for row in df.itertuples() :
        curs.execute('''INSERT INTO Statistiques VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;''',
                    (row.session, row.cod_uai, row.form_lib_voe_acc, row.capa_fin, row.voe_tot, row.voe_tot_f, row.nb_voe_pp, 
                    row.nb_voe_pp_bg, row.nb_voe_pp_bg_brs, row.nb_voe_pp_bt, row.nb_voe_pp_bt_brs, row.nb_voe_pp_bp, row.nb_voe_pp_bp_brs,row.nb_voe_pp_at, row.nb_voe_pc,
                    row.nb_voe_pc_bg, row.nb_voe_pc_bt, row.nb_voe_pc_bp, row.nb_voe_pc_at, row.nb_cla_pp, row.nb_cla_pc, 
                    row.nb_cla_pp_bg, row.nb_cla_pp_bg_brs, row.nb_cla_pp_bt, row.nb_cla_pp_bt_brs, row.nb_cla_pp_bp, 
                    row.nb_cla_pp_bp_brs, row.nb_cla_pp_at, row.prop_tot, row.prop_tot_bg, row.prop_tot_bg_brs,
                    row.prop_tot_bt, row.prop_tot_bt_brs, row.prop_tot_bp, row.prop_tot_bp_brs, row.prop_tot_at, 
                    row.acc_tot, row.acc_tot_f, row.acc_pp, row.acc_pc, row.acc_brs, row.acc_finpp,
                    row.acc_neobac, row.acc_bg, row.acc_bt, row.acc_bp, row.acc_at, row.acc_mention_nonrenseignee,
                    row.acc_sansmention, row.acc_ab, row.acc_b, row.acc_tb, row.acc_tbf, 
                    row.pct_f, row.pct_aca_orig, row.pct_bours, row.pct_acc_debutpp, row.pct_acc_finpp,
                    row.pct_neobac, row.pct_mention_nonrenseignee, row.pct_sansmention, row.pct_ab,
                    row.pct_b, row.pct_tb, row.pct_tbf, row.pct_bg, row.pct_bg_mention, row.pct_bt,
                    row.pct_bt_mention, row.pct_bp, row.pct_bp_mention, row.taux_acces_ens,
                    row.part_acces_gen, row.part_acces_tec, row.part_acces_pro, row.cod_aff_form))

    # On informe que les données ont bien été chargées
    print(" -> Chargement des données dans Statistiques réalisé ")

    print()
    print(" --------------------------------------- ")
    print()



# =======================
# Affichage des données :
# =======================

    # Une fois que les tables sont créées et que toutes les données sont chargées, 
    # on affiche l'entièreté du contenu des tables pour vérifier que tout s'est bien 
    # déroulé 

    vision = pd.read_sql('''
                            SELECT * FROM Etablissement;
                        ''', con = co )
    print(vision)
    print()
    print()

    vision = pd.read_sql('''
                            SELECT * FROM Departement;
                        ''', con = co )
    print(vision)
    print()
    print()

    vision = pd.read_sql('''
                            SELECT * FROM Region;
                        ''', con = co )
    print(vision)
    print()
    print()

    vision = pd.read_sql('''
                            SELECT * FROM Formation;
                        ''', con = co )
    print(vision)
    print()
    print()

    vision = pd.read_sql('''
                            SELECT * FROM Statistiques;
                        ''', con = co )
    print(vision)
    print()
    print()


    print()
    print(" --------------------------------------- ")
    print()



# ======================
# Fermeture du curseur : 
# ======================

    co.commit()
    res = curs.fetchall()

    curs = co.cursor()
    
    curs.close()

except( Exception, psy.DatabaseError ) as error :
    print( error )

finally :
    if co is not None :
        co.close()