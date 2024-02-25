import pandas as pd
import psycopg2 #conectar a postgres (especificamente)
from sqlalchemy import create_engine

#DB parametros
db_params = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "Zaq12wsX$"
}
#conexion con postgres
conect = psycopg2.connect(
    host=db_params["host"],
    database=db_params["database"],
    user=db_params["user"],
    password=db_params["password"]
)
cursor = conect.cursor()
conect.set_session(autocommit = True)
cursor.execute("CREATE DATABASE proyectoBD")

#poner los cmabios
conect.commit()
cursor.close()
conect.close()

#conexion DB
db_params["database"] = "proyectoBD"
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}/{db_params['database']}")

#archivos a convertir (colocar el path de cada CSV segun se requiera)
csv = {
    "appearances":r"C:\Users\abbyd\OneDrive - Universidad del Valle de Guatemala\5to semestre\Datos\Proyecto\appearances.csv",
    "games": r"C:\Users\abbyd\OneDrive - Universidad del Valle de Guatemala\5to semestre\Datos\Proyecto\games.csv",
    "leagues": r"C:\Users\abbyd\OneDrive - Universidad del Valle de Guatemala\5to semestre\Datos\Proyecto\leagues.csv",
    "players": r"C:\Users\abbyd\OneDrive - Universidad del Valle de Guatemala\5to semestre\Datos\Proyecto\players.csv",
    "shots": r"C:\Users\abbyd\OneDrive - Universidad del Valle de Guatemala\5to semestre\Datos\Proyecto\shots.csv",
    "teams": r"C:\Users\abbyd\OneDrive - Universidad del Valle de Guatemala\5to semestre\Datos\Proyecto\teams.csv",
    "teamstats": r"C:\Users\abbyd\OneDrive - Universidad del Valle de Guatemala\5to semestre\Datos\Proyecto\teamstats.csv"
}

#Vereficando que salio bien
for table_name, file_path in csv.items():
    print(f"CSV'{table_name}':")
    df = pd.read_csv(file_path)
    print(df.head(2)) #solo mostrar una parte para checar
    print("\n")
    
#importacion :D
for table_name, file_path in csv.items():
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
