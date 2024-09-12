import psycopg2

try:
    conn=psycopg2.connect(
        host='localhost',
        database="elt",
        user="postgres",
        password="python3.3.3"
    )
except (Exception, psycopg2.Error) as error:
    print("Erreur lors de la connexion à PostgreSQL:", error)

finally:
    # Fermer la connexion
    if conn:
        conn.close()
        print("La connexion PostgreSQL est fermée")