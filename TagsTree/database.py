import mysql.connector
from mysql.connector import errorcode


class DbConnection:

    def __init__(self, database, user, password, host):
        self.__database = database
        self.__user = user
        self.__password = password
        self.__host = host

    def db_try_to_connect(self):
        try:
            return mysql.connector.connect(database=self.__database, user=self.__user, password=self.__password,
                                           host=self.__host)

        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)

    @staticmethod
    def get_projects(project_id):
        project_query = f'SELECT id FROM project WHERE project_id = {project_id}'
        with conn.cursor() as curs:
            try:
                curs.execute(project_query)
                return curs.fetchall()
            except mysql.connector.ProgrammingError as err:
                if errorcode.ER_NO_SUCH_TABLE == err.errno:
                    print("No table exists")
                else:
                    print("Table exists")
                    print(err)

            except mysql.connector.Error as err:
                print("Some other error")
                print(err)

    @staticmethod
    def get_publications(proj_id):
        publication_query = f'''SELECT content FROM publication 
                                INNER JOIN publications_in_project 
                                ON publication.publication_id  = publications_in_project.publications_id 
                                INNER JOIN project
                                ON publications_in_project.project_id  = project.id
                                WHERE publications_in_project.project_id = {proj_id};'''
        with conn.cursor() as curs:
            try:
                curs.execute(publication_query)
                return curs.fetchall()
            except mysql.connector.ProgrammingError as err:
                if errorcode.ER_NO_SUCH_TABLE == err.errno:
                    print("No table exists")
                else:
                    print("Table exists")
                    print(err)

            except mysql.connector.Error as err:
                print("Some other error")
                print(err)

    @staticmethod
    def insert_new_project(project_collections):
        with conn.cursor() as curs:

            project_query = f"INSERT INTO project (project_id, description, period) VALUES (%s, %s, %s) "
            try:
                match len(project_collections):
                    case 1:
                        curs.execute(project_query, project_collections[0])
                    case _:
                        curs.executemany(project_query, project_collections)
                conn.commit()
                print(f"{len(project_collections)} project(s) has/have been successfully inserted")
            except (mysql.connector.IntegrityError, mysql.connector.DataError) as err:
                print("DataError or IntegrityError")
                print(err)

            except mysql.connector.ProgrammingError as err:
                print("Programming Error")
                print(err)

            except mysql.connector.Error as err:
                print(err)

    @staticmethod
    def insert_new_tags(tags_collections):
        with conn.cursor() as curs:

            tags_query = (
                f"""INSERT INTO tags (project_id, 
                                    tag1, tag1_count, 
                                    tag2, tag2_count,
                                    tag3, tag3_count, 
                                    tag4, tag4_count,
                                    tag5, tag5_count,
                                    tag6, tag6_count,
                                    tag7, tag7_count,
                                    tag8, tag8_count,
                                    tag9, tag9_count,
                                    tag10, tag10_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        
                    ON DUPLICATE KEY UPDATE 
                    tag1 = VAlUES(tag1), tag1_count = VALUES(tag1_count), 
                    tag2 = VAlUES(tag2), tag2_count = VAlUES(tag2_count),
                    tag3 = VAlUES(tag3), tag3_count = VAlUES(tag3_count),
                    tag4 = VAlUES(tag4), tag4_count = VAlUES(tag4_count),
                    tag5 = VAlUES(tag5), tag5_count = VAlUES(tag5_count),
                    tag6 = VAlUES(tag6), tag6_count = VAlUES(tag6_count),
                    tag7 = VAlUES(tag7), tag7_count = VAlUES(tag7_count),
                    tag8 = VAlUES(tag8), tag8_count = VAlUES(tag8_count),
                    tag9 = VAlUES(tag9), tag9_count = VAlUES(tag9_count),
                    tag10 = VAlUES(tag10), tag10_count = VAlUES(tag10_count)""")

            try:
                curs.execute(tags_query, tags_collections)
                conn.commit()
                print(f"{len(tags_collections)} tags have been successfully inserted")
            except (mysql.connector.IntegrityError, mysql.connector.DataError) as err:
                print("DataError or IntegrityError")
                print(err)

            except mysql.connector.ProgrammingError as err:
                print("Programming Error")
                print(err)

            except mysql.connector.Error as err:
                print(err)


    @staticmethod
    def insert_into_publication_and_publications_in_project(project_id, period, collection):

        with conn.cursor() as curs:
            try:
                proj_id = curs.execute(f'SELECT id FROM project WHERE period=%s AND project_id=%s',
                                       (period, project_id,))
                proj_id = curs.fetchone()[0]
            except mysql.connector.ProgrammingError as err:
                if errorcode.ER_NO_SUCH_TABLE == err.errno:
                    print("No table exists")
                else:
                    print(err)
            except mysql.connector.Error as err:
                print("Some other error")
                print(err)

            try:

                curs.execute("BEGIN")
                publication_query = f"INSERT INTO publication (title, content, data, source) VALUES (%s, %s, %s, %s) "
                publication_in_project_query = f"INSERT INTO publications_in_project (project_id, publications_id) VALUES (%s, %s)"

                try:
                    curs.execute(publication_query, collection)
                    newid = curs.lastrowid
                except (mysql.connector.IntegrityError, mysql.connector.DataError) as err:
                    print("DataError or IntegrityError")
                    print(err)

                except mysql.connector.ProgrammingError as err:
                    print("Programming Error")
                    print(err)

                except mysql.connector.Error as err:
                    print(err)

                try:
                    publication_in_project_collection = (proj_id, newid)
                    curs.execute(publication_in_project_query, publication_in_project_collection)
                except (mysql.connector.IntegrityError, mysql.connector.DataError) as err:
                    print("DataError or IntegrityError")
                    print(err)

                except mysql.connector.ProgrammingError as err:
                    print("Programming Error")
                    print(err)

                except mysql.connector.Error as err:
                    print(err)

                conn.commit()
            except print('conn begin fail'):
                try:  # empty exception handler in case rollback fails
                    conn.rollback()
                    print('rallback')
                except:
                    pass
            else:
                print(f"{len(collection)} records inserted successfully")


dbconnection = DbConnection(database='tagclouddb', user='gastinha', password='Gastinh@', host='localhost')
conn = dbconnection.db_try_to_connect()
