import sqlite3
import os
class DatabaseManager:
    def __init__(self, db_name):
        self.db_name = db_name
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

    def save_metadata(self):
        metadata_connection = sqlite3.connect("metadata.db")
        metadata_cursor = metadata_connection.cursor()

        # Insert database name into DATABASES table
        metadata_cursor.execute("INSERT OR IGNORE INTO DATABASES VALUES (?);", (self.db_name,))
        metadata_connection.commit()

        # Insert table names into TABLES table
        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        self.cursor.execute(tables_query)
        tables = self.cursor.fetchall()

        for table in tables:
            table_name = table[0]
            metadata_cursor.execute("INSERT OR IGNORE INTO TABLES VALUES (?, ?);", (table_name, self.db_name))
            metadata_connection.commit()

            # Insert columns metadata into COLUMNS table
            columns_query = f"PRAGMA table_info({table_name});"
            self.cursor.execute(columns_query)
            columns = self.cursor.fetchall()

            # Get unique columns
            unique_columns_query = f"PRAGMA index_list({table_name});"
            self.cursor.execute(unique_columns_query)
            index_list = self.cursor.fetchall()

            unique_columns = []
            for index_info in index_list:
                index_name = index_info[1]
                index_info_query = f"PRAGMA index_info({index_name});"
                self.cursor.execute(index_info_query)
                index_columns = [col[2] for col in self.cursor.fetchall()]
                unique_columns.extend(index_columns)

            for col in columns:
                col_name, col_type, not_null,dflt_value, pk = col[1], col[2], col[3], col[4], col[5]
                # default_value_query = f"SELECT {col_name} FROM {table_name} WHERE {col_name} IS NOT NULL LIMIT 1;"
                # self.cursor.execute(default_value_query)
                # default_value = self.cursor.fetchone()
                unique_indicator = "   1" if col_name in unique_columns else "   0"
                # # Handle cases where values are None
                # not_null = 1 if not_null else 0
                # pk = 1 if pk == 1 else 0
                # default_value = default_value[0] if default_value else None

                metadata_cursor.execute("INSERT OR IGNORE INTO COLUMNS VALUES (?, ?, ?, ?, ?, ?, ?);",
                                    (col_name, table_name, col_type, pk, not_null, unique_indicator,dflt_value))
                metadata_connection.commit()

            # Insert foreign keys into FOREIGN_KEY table
            foreign_keys_query = f"PRAGMA foreign_key_list({table_name});"
            self.cursor.execute(foreign_keys_query)
            foreign_keys = self.cursor.fetchall()

            for fk in foreign_keys:
                col_name, ref_table, ref_col = fk[3], fk[2], fk[4]
                metadata_cursor.execute("INSERT OR IGNORE INTO FOREIGN_KEY VALUES (?,?, ?);", (col_name,ref_table, ref_col))
                metadata_connection.commit()

    def table_exists(self, table_name):
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
        self.cursor.execute(query, (table_name,))
        return self.cursor.fetchone() is not None

    def column_exists(self, table_name, column_name):
        query = f"PRAGMA table_info({table_name});"
        self.cursor.execute(query)
        columns = [col[1] for col in self.cursor.fetchall()]
        return column_name in columns

    def create_table(self, table_name, columns, foreign_keys=None):
        # Check if the table already exists
        if self.table_exists(table_name):
            print(f"Table {table_name} already exists.")
            return

        # Check if referenced table and column exist for each foreign key
        for fk in (foreign_keys or []):
            referenced_table, referenced_column = fk['table'], fk['referenced_column']
            if not self.table_exists(referenced_table):
                print(f"Referenced table {referenced_table} does not exist.")
                return
            if not self.column_exists(referenced_table, referenced_column):
                print(f"Referenced column {referenced_column} in table {referenced_table} does not exist.")
                return

        # Build columns string
        columns_str = ', '.join(columns)

        # Build foreign keys string
        foreign_keys_str = ', '.join(
            [f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['table']}({fk['referenced_column']})" for fk in
             (foreign_keys or [])])

        # Combine columns and foreign keys in the CREATE TABLE query
        if foreign_keys_str == '':
            query = f"CREATE TABLE {table_name} ({columns_str})"
        else:
            query = f"CREATE TABLE {table_name} ({columns_str}, {foreign_keys_str})"

        # Execute the query
        self.cursor.execute(query)
        self.connection.commit()
        print(f"Table {table_name} created successfully.")

    def insert_data(self, table_name, values):
        # Check if the table exists
        if not self.table_exists(table_name):
            print(f"Table {table_name} does not exist.")
            return

        # Build the INSERT query
        columns_str = ', '.join(values.keys())
        values_str = ', '.join(['?' for _ in values.values()])
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"

        # Execute the query
        self.cursor.execute(query, tuple(values.values()))
        self.connection.commit()
        print("Data inserted successfully.")
    def close_connection(self):
        self.connection.close()

    def show_database_metadata(self):
        # Fetch and print tables, columns, and constraints information
        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        self.cursor.execute(tables_query)
        tables = self.cursor.fetchall()

        for table in tables:
            table_name = table[0]
            print(f"\n\nTable: {table_name}")

            # Columns
            columns_query = f"PRAGMA table_info({table_name});"
            self.cursor.execute(columns_query)
            columns = self.cursor.fetchall()

            # Get primary key columns
            pk_columns_query = f"PRAGMA table_info({table_name});"
            self.cursor.execute(pk_columns_query)
            pk_columns = [col[1] for col in self.cursor.fetchall() if col[5] == 1]  # col[5] is the "pk" column

            # Get unique columns
            unique_columns_query = f"PRAGMA index_list({table_name});"
            self.cursor.execute(unique_columns_query)
            index_list = self.cursor.fetchall()

            unique_columns = []
            for index_info in index_list:
                index_name = index_info[1]
                index_info_query = f"PRAGMA index_info({index_name});"
                self.cursor.execute(index_info_query)
                index_columns = [col[2] for col in self.cursor.fetchall()]
                unique_columns.extend(index_columns)

            unique_columns = list(set(unique_columns))  # Remove duplicates

            # Print header for columns
            print("{:<15} {:<15} {:<8} {:<15} {:<3} {:<6}".format("name", "type", "notnull", "  dflt_value", "pk",
                                                                  "unique"))
            print("-" * 70)


            for col in columns:
                cid, name, data_type, not_null, default_value, pk = col

                # Handle cases where values are None
                not_null = "   1" if not_null else "   0"
                default_value = f"      {default_value}" if default_value is not None else "    none"
                pk_indicator = "1" if name in pk_columns else "0"
                unique_indicator = "   1" if name in unique_columns else "   0"

                print("{:<15} {:<15} {:<8} {:<15} {:<3} {:<6}".format(name, data_type, not_null, default_value,
                                                                      pk_indicator, unique_indicator))

            # Foreign Keys
            foreign_keys_query = f"PRAGMA foreign_key_list({table_name});"
            self.cursor.execute(foreign_keys_query)
            foreign_keys = self.cursor.fetchall()

            if foreign_keys:
                print("\nForeign Keys:")
                for fk in foreign_keys:
                    print(f"{fk[3]} references {fk[2]}({fk[4]})\n")

    def show_table_data(self, table_name):
        # Check if the table exists
        if not self.table_exists(table_name):
            print(f"\nTable {table_name} does not exist.")
            return

        # Fetch and print data from the table
        query = f"SELECT * FROM {table_name};"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        if not rows:
            print(f"No data found in table {table_name}.")
            return

        # Get column names
        column_names = [description[0] for description in self.cursor.description]

        # Calculate column widths based on the maximum length of header and data values
        col_widths = [max(len(str(header)), max(len(str(row[i])) for row in rows)) for i, header in enumerate(column_names)]

        # Print table header
        header = " | ".join(f"{header:<{width}}" for header, width in zip(column_names, col_widths))
        print(header)
        print("-" * sum(col_widths + [len(column_names) - 1] * 3))  # separator line

        # Print data rows
        for row in rows:
            row_str = " | ".join(f"{str(value):<{width}}" for value, width in zip(row, col_widths))
            # print(row_str)
            print(row_str)
def show_existing_databases():
    print("\nExisting Databases:")
    for filename in os.listdir():
        if filename.endswith(".db"):
            print(filename)
# ----------------------------------------------------------------------------------------------------------------------
# Create metadata.db and necessary tables if they don't exist
metadata_db_name = "metadata.db"

metadata_connection = sqlite3.connect(metadata_db_name)
metadata_cursor = metadata_connection.cursor()

# Create DATABASES table
metadata_cursor.execute("""
    CREATE TABLE IF NOT EXISTS DATABASES (
        name TEXT PRIMARY KEY
    );
""")

# Create TABLES table
metadata_cursor.execute("""
    CREATE TABLE IF NOT EXISTS TABLES (
        name TEXT PRIMARY KEY,
        database TEXT,
        FOREIGN KEY (database) REFERENCES DATABASES(name)
    );
""")

# Create COLUMNS table
metadata_cursor.execute("""
    CREATE TABLE IF NOT EXISTS COLUMNS (
        name TEXT,
        table_name TEXT,
        type TEXT,
        pk INTEGER,
        not_null INTEGER,
        unique_col INTEGER,
        dflt_val TEXT,
        PRIMARY KEY (name, table_name),
        FOREIGN KEY (table_name) REFERENCES TABLES(name)
    );
""")

# Create FOREIGN_KEY table
metadata_cursor.execute("""
    CREATE TABLE IF NOT EXISTS FOREIGN_KEY (
        col TEXT,
        refrenced_table TEXT,
        refrenced_col TEXT,
        PRIMARY KEY (col),
        FOREIGN KEY (col) REFERENCES COLUMNS(name),
        FOREIGN KEY (refrenced_table) REFERENCES TABLES(name)
        FOREIGN KEY (refrenced_col) REFERENCES COLUMNS(name)
    );
""")

metadata_connection.commit()
metadata_connection.close()
################################################################################################################
# Creating an example database
db_manager = DatabaseManager("COMPANY.db")
connection = sqlite3.connect("COMPANY.db")
cursor = connection.cursor()


cursor.execute("""
CREATE TABLE EMPLOYEE
( Fname           VARCHAR(10)   NOT NULL,
  Minit           CHAR,
  Lname           VARCHAR(20)      NOT NULL,
  Ssn             CHAR(9)          NOT NULL,
  Bdate           DATE,
  Address         VARCHAR(30),
  Sex             CHAR(1),
  Salary          DECIMAL(5),
  Super_ssn       CHAR(9),
  Dno             INT               NOT NULL,
PRIMARY KEY   (Ssn),
FOREIGN KEY (Super_ssn) REFERENCES EMPLOYEE(Ssn),
FOREIGN KEY  (Dno) REFERENCES DEPARTMENT(Dnumber));
""")
cursor.execute("""CREATE TABLE DEPARTMENT
( Dname           VARCHAR(15)       NOT NULL,
  Dnumber         INT               NOT NULL,
  Mgr_ssn         CHAR(9)           NOT NULL,
  Mgr_start_date  DATE,
PRIMARY KEY (Dnumber),
UNIQUE      (Dname),
FOREIGN KEY (Mgr_ssn) REFERENCES EMPLOYEE(Ssn) );""")

cursor.execute("""CREATE TABLE DEPT_LOCATIONS
( Dnumber         INT               NOT NULL,
  Dlocation       VARCHAR(15)       NOT NULL,
PRIMARY KEY (Dnumber, Dlocation),
FOREIGN KEY (Dnumber) REFERENCES DEPARTMENT(Dnumber) );
""")
cursor.execute("""CREATE TABLE PROJECT
( Pname           VARCHAR(15)       NOT NULL,
  Pnumber         INT               NOT NULL,
  Plocation       VARCHAR(15),
  Dnum            INT               NOT NULL,
PRIMARY KEY (Pnumber),
UNIQUE      (Pname),
FOREIGN KEY (Dnum) REFERENCES DEPARTMENT(Dnumber) );
""")
cursor.execute("""CREATE TABLE WORKS_ON
( Essn            CHAR(9)           NOT NULL,
  Pno             INT               NOT NULL,
  Hours           DECIMAL(3,1)      NOT NULL,
PRIMARY KEY (Essn, Pno),
FOREIGN KEY (Essn) REFERENCES EMPLOYEE(Ssn),
FOREIGN KEY (Pno) REFERENCES PROJECT(Pnumber) );""")

cursor.execute("""CREATE TABLE DEPENDENT
( Essn            CHAR(9)           NOT NULL,
  Dependent_name  VARCHAR(15)       NOT NULL,
  Sex             CHAR,
  Bdate           DATE,
  Relationship    VARCHAR(8),
PRIMARY KEY (Essn, Dependent_name),
FOREIGN KEY (Essn) REFERENCES EMPLOYEE(Ssn) );
""")
cursor.execute("""INSERT INTO EMPLOYEE
VALUES      ('John','B','Smith',123456789,'1965-01-09','731 Fondren, Houston TX','M',30000,333445555,5),
            ('Franklin','T','Wong',333445555,'1965-12-08','638 Voss, Houston TX','M',40000,888665555,5),
            ('Alicia','J','Zelaya',999887777,'1968-01-19','3321 Castle, Spring TX','F',25000,987654321,4),
            ('Jennifer','S','Wallace',987654321,'1941-06-20','291 Berry, Bellaire TX','F',43000,888665555,4),
            ('Ramesh','K','Narayan',666884444,'1962-09-15','975 Fire Oak, Humble TX','M',38000,333445555,5),
            ('Joyce','A','English',453453453,'1972-07-31','5631 Rice, Houston TX','F',25000,333445555,5),
            ('Ahmad','V','Jabbar',987987987,'1969-03-29','980 Dallas, Houston TX','M',25000,987654321,4),
            ('James','E','Borg',888665555,'1937-11-10','450 Stone, Houston TX','M',55000,null,1);
""")
cursor.execute("""INSERT INTO DEPARTMENT
VALUES      ('Research',5,333445555,'1988-05-22'),
            ('Administration',4,987654321,'1995-01-01'),
            ('Headquarters',1,888665555,'1981-06-19');
""")
cursor.execute("""INSERT INTO PROJECT
VALUES      ('ProductX',1,'Bellaire',5),
            ('ProductY',2,'Sugarland',5),
            ('ProductZ',3,'Houston',5),
            ('Computerization',10,'Stafford',4),
            ('Reorganization',20,'Houston',1),
            ('Newbenefits',30,'Stafford',4);
""")
cursor.execute("""INSERT INTO WORKS_ON
VALUES     (123456789,1,32.5),
           (123456789,2,7.5),
           (666884444,3,40.0),
           (453453453,1,20.0),
           (453453453,2,20.0),
           (333445555,2,10.0),
           (333445555,3,10.0),
           (333445555,10,10.0),
           (333445555,20,10.0),
           (999887777,30,30.0),
           (999887777,10,10.0),
           (987987987,10,35.0),
           (987987987,30,5.0),
           (987654321,30,20.0),
           (987654321,20,15.0),
           (888665555,20,16.0);
""")
cursor.execute("""INSERT INTO DEPENDENT
VALUES      (333445555,'Alice','F','1986-04-04','Daughter'),
            (333445555,'Theodore','M','1983-10-25','Son'),
            (333445555,'Joy','F','1958-05-03','Spouse'),
            (987654321,'Abner','M','1942-02-28','Spouse'),
            (123456789,'Michael','M','1988-01-04','Son'),
            (123456789,'Alice','F','1988-12-30','Daughter'),
            (123456789,'Elizabeth','F','1967-05-05','Spouse');
""")
cursor.execute("""INSERT INTO DEPT_LOCATIONS
VALUES      (1,'Houston'),
            (4,'Stafford'),
            (5,'Bellaire'),
            (5,'Sugarland'),
            (5,'Houston');
""")
# cursor.execute("""ALTER TABLE DEPARTMENT
#  ADD CONSTRAINT Dep_emp ;
# """)
# cursor.execute("""ALTER TABLE EMPLOYEE
#  ADD CONSTRAINT Emp_emp ;""")
# cursor.execute("""ALTER TABLE EMPLOYEE
#  ADD CONSTRAINT Emp_dno ;""")
# cursor.execute("""ALTER TABLE EMPLOYEE
#  ADD CONSTRAINT Emp_super ;
#  """)
connection.commit()
db_manager.save_metadata()
connection.close()


################################################################################################################
if __name__ == "__main__":
    while True:
        print("\nDatabase Management Menu:")
        print("1. Create a new database")
        print("2. Show Metadata of an existing database")
        print("3. Insert data into an existing table")
        print("4. Show datas of an existing database")
        print("5. exit")

        choice = input("Enter your choice (1/2/3/4/5): ")

        if choice == "1":
            # Option to create a new database
            db_name = input("Enter the name of the new database: ")
            if not db_name.endswith(".db"):
                db_name += ".db"
            db_manager = DatabaseManager(db_name)

            while True:
                table_name = input("Enter table name (or 'back' to go back): ")
                if table_name.lower() == 'back':
                    break

                columns = input("Enter columns (comma-separated): ").split(',')

                # Example foreign key: {"column": "product_id", "table": "products", "referenced_column": "product_id"}
                foreign_keys = []
                while True:
                    fk_input = input("Enter foreign key (column,table,referenced_column; leave blank to finish): ")
                    if not fk_input:
                        break
                    fk_parts = fk_input.split(',')
                    if len(fk_parts) == 3:
                        foreign_keys.append(
                            {"column": fk_parts[0], "table": fk_parts[1], "referenced_column": fk_parts[2]})
                    else:
                        print("Invalid input for foreign key. Try again.")

                db_manager.create_table(table_name, columns, foreign_keys)

            db_manager.connection.commit()
            db_manager.save_metadata()
            db_manager.close_connection()

        elif choice == "2":
            # Option to show information of an existing database
            while True:
                show_existing_databases()

                db_name = input("Enter the name of the existing (or 'back' to go back): ")
                if db_name.lower() == 'back':
                    break

                if not db_name.endswith(".db"):
                    db_name += ".db"

                if os.path.exists(db_name):
                    db_manager = DatabaseManager(db_name)
                    db_manager.show_database_metadata()
                    db_manager.close_connection()
                else:
                    print(f"Database '{db_name}' not found.")

        elif choice == "5":
            # Option to exit the application
            print("Bye ._.")
            break

        elif choice == "3":
            # Option to insert data into an existing table
            while True:
                show_existing_databases()
                db_name = input("Enter the name of the existing database (or 'back' to go back): ")
                if db_name.lower() == 'back':
                    break

                if not db_name.endswith(".db"):
                    db_name += ".db"

                if os.path.exists(db_name):
                    db_manager = DatabaseManager(db_name)
                    db_manager.show_database_metadata()

                    table_name = input("Enter the name of the existing table to insert data: ")
                    if db_manager.table_exists(table_name):
                        # Get the columns of the table
                        columns_query = f"PRAGMA table_info({table_name});"
                        db_manager.cursor.execute(columns_query)
                        columns = [col[1] for col in db_manager.cursor.fetchall()]

                        # Prompt for values for all columns
                        values = {}
                        for col in columns:
                            value = input(f"Enter value for column '{col}': ")
                            values[col] = value

                        db_manager.insert_data(table_name, values)
                    else:
                        print(f"Table '{table_name}' not found.")

                    db_manager.close_connection()
                else:
                    print(f"Database '{db_name}' not found.")
        elif choice == "4":
            # Option to show all tables and data in an existing database
            while True:
                show_existing_databases()
                db_name = input("Enter the name of the existing database (or 'back' to go back): ")
                if db_name.lower() == 'back':
                    break

                if not db_name.endswith(".db"):
                    db_name += ".db"

                if os.path.exists(db_name):
                    db_manager = DatabaseManager(db_name)
                    #db_manager.show_database_info()

                    # Show data for each table
                    tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
                    db_manager.cursor.execute(tables_query)
                    tables = db_manager.cursor.fetchall()

                    for table in tables:
                        table_name = table[0]
                        print(f"\n\nTable: {table_name}")
                        db_manager.show_table_data(table_name)

                    db_manager.close_connection()
                else:
                    print(f"Database '{db_name}' not found.")

        else:
            print("Invalid choice. Please enter a valid option (1/2/3/4/5).")



# todo: add unique constraint,
#       create database doesn't work
#       check the foreign key constraint
#       adding tuples to the tables
#       showing the tables data

