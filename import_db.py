import pdb
import psycopg2
CENSUS_FILE = 'CENSUS_PUB_20160309.txt'

def get_columns():
  columns_file = open('columns.csv', 'rU')
  columns = list()
  for line in columns_file:
    line = line.strip('\r\n')
    columns.append(line)
  return columns

def get_connection():
  try:
      conn = psycopg2.connect("dbname='carrier_directory' user='loaddocs' host='localhost' password='rollout123'")
  except:
      print "I am unable to connect to the database"
  return conn

fmcsa_file = open(CENSUS_FILE, 'r')

columns = get_columns()
conn = get_connection()
cur = conn.cursor()
try:
  cur.execute("CREATE TABLE carriers (" + " TEXT , ".join(columns) + " TEXT)")
  conn.commit()
except psycopg2.ProgrammingError:
  # if the table has already been crated
  print "carriers table has been already created"
  conn.rollback()

row_num = 0
arr = list()
inserted_row_count = 0
for line in fmcsa_file:
  cur = conn.cursor()
  row_num += 1
  carrier_row = line.split('~')
  if len(carrier_row) != len(columns):
    print "Skipping Row: {0}, # of values {1} doesnt is not equal expected # of columns:{2}, {3}".format(row_num, len(carrier_row), len(columns), carrier_row)
    continue
  insert_statement = 'INSERT INTO carriers VALUES (' + ','.join(['%s' for i in range(len(columns))]) + ')';
  try:
    cur.execute(insert_statement, carrier_row)
    conn.commit()
    inserted_row_count += 1
  except psycopg2.DataError as e:
    conn.rollback()
    print "There was a data problem with: {0}".format(carrier_row)
    print e

print "inserted:{0} of {1} rows. We skipped {2} rows".format(inserted_row_count, row_num, row_num - inserted_row_count)
