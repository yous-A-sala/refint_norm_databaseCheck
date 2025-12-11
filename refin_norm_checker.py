# Import the psycopg2 (Python driver that connects to psql)
import psycopg2
import glob
# Import your psql credentials from the python file db_config.py
from db_config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

import re
def parseSchemaFile(filename):
    # initializes dictionary "tables"
    tables = {}
    
    # stores file lines into a list 
    with open(filename, "r") as f:
        lines = f.readlines()

    # removes blank spaces and other special chars
    for i in lines:
        s = i.strip()
        if not s:
            continue

        # regex to capture pattern in files into two parenthesis groups
        m = re.match(r'^([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(.*)\s*\)\s*$', s)
        if not m:
            print(f"Skipping bad line: {s}")
            continue
        tableName = m.group(1)
        inside = m.group(2)


        cols = []
        cur = ""
        depth = 0
        for ch in inside:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            if ch == ',' and depth == 0:
                cols.append(cur.strip())
                cur = ""
            else:
                cur += ch
        if cur.strip():
            cols.append(cur.strip())

        pk = None
        colnames = []
        fks = []

        # parses everything other than table name
        for token in cols:
            token = token.strip()
            mm = re.match(r'^([A-Za-z_][A-Za-z0-9_]*)\s*(?:\(\s*(.+)\s*\))?$', token)
            if not mm:
                print(f"Skipping invalid col: {token}")
                continue
            cname = mm.group(1)
            colnames.append(cname)
            key = mm.group(2)

            # detects any type of key
            if key:
                key = key.strip()
                if key.lower() == 'pk':
                    pk = cname
                elif key.lower().startswith('fk'):
                    parts = key.split(':', 1)
                    if len(parts) == 2:
                        right = parts[1].strip()
                        if '.' in right:
                            refTable, refCol = right.split('.', 1)
                        else:
                            refTable, refCol = right.split(':',1) if ':' in right else (right,right)
                        fks.append((cname, refTable.strip(), refCol.strip()))
        tables[tableName] = {'pk': pk, 'cols': colnames, 'fks': fks}
    return tables


def referentialIntegrity(cursor, tables):
    tableFinal = {}
    dbValid = True

    for tableName, meta in tables.items():
        fks = meta['fks']
        if not fks:
            tableFinal[tableName] = 'Y'
            continue

        tableValid = True
        for fkCol, refTable, refCol in fks:
            query = f"""
                SELECT COUNT(*) FROM {tableName} t
                LEFT JOIN {refTable} r ON t.{fkCol} = r.{refCol}
                WHERE r.{refCol} IS NULL;
                """
            cursor.execute(query)
            missing = cursor.fetchall()[0][0]
            if missing > 0:
                tableValid = False
                break

        tableFinal[tableName] = 'Y' if tableValid else 'N'
        if not tableValid:
            dbValid = False

    return tableFinal, 'Y' if dbValid else 'N'


def normalized(cursor, tables):
    tableFinal = {}
    dbValid = True

    for tableName, meta in tables.items():
        pk = meta['pk']
        cols = [i for i in meta['cols'] if i != pk]
        tableValid = True

        for i in range(len(cols)):
            for j in range(len(cols)):
                if i == j:
                    continue
                A, B = cols[i], cols[j]


                queryA = f"SELECT COUNT(DISTINCT {A}) FROM {tableName};"
                cursor.execute(queryA)
                colA = cursor.fetchone()[0]


                queryAB = f"SELECT COUNT(DISTINCT ROW({A}, {B})) FROM {tableName};"
                cursor.execute(queryAB)
                colB = cursor.fetchone()[0]
                
                
                if colA == colB:
                    tableValid = False
                    break
            if not tableValid:
                break
        tableFinal[tableName] = 'Y' if tableValid else 'N'
        if not tableValid:
            dbValid = False
            
    return tableFinal, 'Y' if dbValid else 'N'

# glob retuns a list of files that satisfies this pattern
sqlFiles = sorted(glob.glob("tc*.sql"))
# parses all sql files's ending ".sql"
testcases = [fName[:-4] for fName in sqlFiles]

connection = None
cursor = None

try:
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = connection.cursor()

    for i in testcases:
        sqlFile = f"{i}.sql"
        schemaFile = f"{i}.txt"
        outputFile = f"{i}.out"

        with open(sqlFile,"r") as f:
            sFile = f.read()
        cursor.execute(sFile)
        connection.commit()

        tables = parseSchemaFile(schemaFile)
        tableRI, dbRI = referentialIntegrity(cursor, tables)
        tableNorm, dbNorm = normalized(cursor, tables)


        with open(outputFile, "w") as f:
            f.write("referential integrity normalized\n")
            for t in sorted(tables.keys()):
                f.write(f"{t:<15} {tableRI[t]:<10} {tableNorm[t]}\n")
            f.write(f"DB referential integrity: {dbRI} \n")
            f.write(f"DB normalized: {dbNorm}\n")

        print(f"Success {outputFile}")

        #dropTable = "" test
        for tableName in tables.keys():
            cursor.execute(f"DROP TABLE IF EXISTS {tableName} CASCADE;")
        connection.commit()

# Avoid memory leak and free resources
finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
