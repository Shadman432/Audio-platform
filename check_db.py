import psycopg2
import sys

try:
    conn = psycopg2.connect('postgresql://neondb_owner:npg_yrofhle9ZU4D@ep-late-dawn-a1orve1s-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require')
    cur = conn.cursor()
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'stories' AND column_name = 'subgenre'")
    columns = cur.fetchall()
    if columns:
        print('The subgenre column exists in the stories table.')
    else:
        print('The subgenre column does not exist in the stories table.')
    cur.close()
    conn.close()
except Exception as e:
    print('Error:', e, file=sys.stderr)
