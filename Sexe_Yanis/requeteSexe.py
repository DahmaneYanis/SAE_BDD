import psycopg2 as psy
import pandas as pd

try :
    ### Connection
    co = psy.connect(host = 'londres',
                    database = 'dbjemarcilla',
                    user = 'jemarcilla',
                    password = "achanger")

    ### Requête pour Graphique1
    df = pd.read_sql('''SELECT pct_f, type 
                        from Statistiques''', con=co)
    print("test")
    df.to_csv("csvSexe/Graphique1.csv") # Exportation du résultat de la requête en .csv

    ### Requête pour Graphique2
    df = pd.read_sql('''SELECT pct_f, effectif_candidats_total, effectif_candidats_femmes_total 
                        from Statistiques''', con=co)
    
    df.to_csv("csvSexe/Graphique2.csv") # Exportation du résultat de la requête en .csv

    ### Requête pour Graphique3
    df = pd.read_sql('''SELECT pct_f, effectifs_candidats_total_accepte, effectifs_candidats_femmes_total_accepte 
                        from Statistiques''', con=co)
    
    df.to_csv("csvSexe/Graphique3.csv") # Exportation du résultat de la requête en .csv
    
except (Exception , psy. DatabaseError ) as error :
    print(error)
