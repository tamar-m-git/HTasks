import sqlite3
import os

def createDB(Path_DB):
    if os.path.exists(PathDB):
        os.remove(PathDB)
    conn = sqlite3.connect(PathDB)
    conn.execute('PRAGMA foreign_keys = ON;')
    cursor = conn.cursor()
    #create table of person
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Person (
        Person_id   TEXT    PRIMARY KEY,
        First_Name  TEXT    NOT NULL,
        Last_Name   TEXT    NOT NULL,
        Gender TEXT CHECK(gender IN ('male', 'female')),
        Father_id   TEXT,
        Mother_id   TEXT,
        Spouse_id   TEXT,
        FOREIGN KEY (Father_id) REFERENCES Person(Person_id),
        FOREIGN KEY (Mother_id) REFERENCES Person(Person_id),
        FOREIGN KEY (Spouse_id) REFERENCES Person(Person_id)
    )
    """)
    sql_insert = """
    INSERT INTO Person (
        Person_id,
        First_Name,
        Last_Name,
        Gender,
        Father_id,
        Mother_id,
        Spouse_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    people = [
        ('121232332', 'Yael', 'Bitan', 'female', None, None, None),
        ('787878898', 'Gadi', 'Bitan', 'male', None, None, '121232332'),
        ('040166968', 'David', 'Bitan', 'male', '787878898', '121232332', None),
        ('556332556', 'Tamar', 'Bitan', 'female', '787878898', '121232332', None),
        ('589658997', 'Shir', 'Bitan', 'female', '787878898', '121232332', None),
        ('010222030', 'Natan', 'Yanay', 'male', None, None, None),
        ('755666325', 'Rachel', 'Yanay', 'female', '787878898', '121232332', '010222030'),
        ('212563332', 'Yuval', 'Yanay', 'male', '010222030', '755666325', None),

    ]
    NewPeople=[  ('998669889', 'roni', 'maor', 'male', None, None, None),
        ('877986581', 'Chen', 'maor', 'female', None, None, '998669889'),
        ('223556998', 'pini', 'catz', 'male', None, None, None),
        ('323658569', 'tal', 'catz', 'female', None, None, '223556998'),
         ]

    cursor.executemany(sql_insert, people)
    cursor.executemany(sql_insert, NewPeople)
    cursor.execute("UPDATE Person SET Spouse_id = 787878898 WHERE Person_Id = 121232332;")
    cursor.execute("UPDATE Person SET Spouse_id = 755666325 WHERE Person_Id = 010222030;")
    #View table of Connection kinds
    cursor.execute( """ CREATE VIEW IF NOT EXISTS ShowConnectionRelative AS
SELECT p.Person_id  AS personId,p.Father_id  AS relativeId,'Father' AS connection
FROM Person p
WHERE p.Father_id IS NOT NULL

UNION ALL

SELECT 
p.Person_id  AS personId,p.Mother_id  AS relativeId,'Mother' AS connection
FROM Person p
WHERE p.Mother_id IS NOT NULL

UNION ALL

SELECT 
  p.Person_id  AS personId,
  p.Spouse_id  AS relativeId,
  CASE p.Gender
    WHEN 'male'   THEN 'Spouse(female)'
    WHEN 'female' THEN 'Spouse(male)'
  END          AS connection
FROM Person p
WHERE p.Spouse_id IS NOT NULL

UNION ALL

SELECT 
  p1.Person_id AS personId,
  p2.Person_id AS relativeId,
  CASE 
    WHEN p2.Gender = 'male'   THEN 'brother'
    WHEN p2.Gender = 'female' THEN 'sister'
  END  AS connection
FROM Person p1
JOIN Person p2
  ON (p1.Father_id = p2.Father_id AND p1.Father_id IS NOT NULL)
  OR (p1.Mother_id = p2.Mother_id AND p1.Mother_id IS NOT NULL)
WHERE p1.Person_id <> p2.Person_id

UNION ALL

SELECT 
  p1.Person_id AS personId,
  c.Person_id  AS relativeId,
  CASE 
    WHEN c.Gender = 'male'   THEN 'Son'
    WHEN c.Gender = 'female' THEN 'Daughter'
  END  AS connection
FROM Person p1
JOIN Person c
  ON c.Father_id = p1.Person_id
  OR c.Mother_id = p1.Person_id;
 """)

    conn.commit()
    conn.close()


import sqlite3
import os
def completeSpouse_id(pathDB):
    conn = sqlite3.connect(pathDB)
    cursor = conn.cursor()
    cursor.execute("""
    UPDATE Person
     SET Spouse_id = (
         SELECT p2.Person_id
         FROM Person AS p2
         WHERE p2.Spouse_id = Person.Person_id
         LIMIT 1
     )
     WHERE Spouse_id IS NULL
       AND Person_id IN (
           SELECT Spouse_id
           FROM Person
           WHERE Spouse_id IS NOT NULL
       );
     """)
    conn.commit()
    conn.close()



def show_tables(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    print("Person")
    cursor.execute("SELECT * FROM Person")
    rows = cursor.fetchall()
    cols = [c[0] for c in cursor.description]
    print(cols)
    for row in rows:
        print(row)

    print("\n ShowConnectionRelative")
    cursor.execute("SELECT * FROM ShowConnectionRelative")
    rows = cursor.fetchall()
    cols = [c[0] for c in cursor.description]
    print(cols)
    for row in rows:
        print(row)
    conn.close()


if __name__ == "__main__":
    Base_Path = os.path.dirname(__file__)
    PathDB = os.path.join(Base_Path, 'familyTree.db')
    os.makedirs(os.path.dirname(PathDB), exist_ok=True)
    createDB(Path_DB=PathDB)
    print("before fix")
    show_tables(PathDB)
    completeSpouse_id(PathDB)
    show_tables(PathDB)

