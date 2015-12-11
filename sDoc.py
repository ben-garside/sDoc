#!Python 3

# Author        Benjamin Garside
# Created       2015-10-29
# Contact       abgarside@gmail.com

from __future__ import print_function
import pypyodbc


class DB:

    def __init__(self, connType=''):
        self.server = ''
        self.uid = ''
        self.pwd = ''
        if len(connType):
            self.connectionType = connType
        else:
            self.connectionType = 'sqlsrv' # set default
        self.catalog = ''

    def newConn(self):
        return pypyodbc.connect("DRIVER={SQL Server};"
                                "SERVER="+self.server+";"
                                "UID="+self.uid+";"
                                "PWD="+self.pwd+";"
                                "DATABASE="+self.catalog+"")

    def getDocumentation(self):
        conn = self.newConn()
        cur = conn.cursor()
        cur.execute(
            '''
            SELECT  c.TABLE_CATALOG AS CatalogName,
                    c.TABLE_SCHEMA AS SchemaName,
                    c.TABLE_NAME AS TableName,
                    t.TABLE_TYPE AS TableType,
                    c.ORDINAL_POSITION AS Ordinal,
                    c.COLUMN_NAME AS ColumnName,
                    q.[epExtendedProperty] tableDescription,
                    qt.[epExtendedProperty] columnDescription,
                    CAST(CASE WHEN IS_NULLABLE = 'YES' THEN 1
                              ELSE 0
                         END AS BIT) AS IsNullable,
                    DATA_TYPE AS TypeName,
                    ISNULL(CHARACTER_MAXIMUM_LENGTH, 0) AS [MaxLength],
                    CAST(ISNULL(NUMERIC_PRECISION, 0) AS INT) AS [Precision],
                    ISNULL(COLUMN_DEFAULT, '') AS [Default],
                    CAST(ISNULL(DATETIME_PRECISION, 0) AS INT) AS DateTimePrecision,
                    ISNULL(NUMERIC_SCALE, 0) AS Scale,
                    CAST(COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') AS BIT) AS IsIdentity,
                    CAST(CASE WHEN COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') = 1 THEN 1
                              WHEN COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsComputed') = 1 THEN 1
                              WHEN DATA_TYPE = 'TIMESTAMP' THEN 1
                              ELSE 0
                         END AS BIT) AS IsStoreGenerated,
                    CAST(CASE WHEN pk.ORDINAL_POSITION IS NULL THEN 0
                              ELSE 1
                         END AS BIT) AS PrimaryKey,
                    ISNULL(pk.ORDINAL_POSITION, 0) PrimaryKeyOrdinal,
                    CAST(CASE WHEN fk.COLUMN_NAME IS NULL THEN 0
                              ELSE 1
                         END AS BIT) AS IsForeignKey
            FROM    INFORMATION_SCHEMA.COLUMNS c
                    LEFT OUTER JOIN (SELECT u.TABLE_SCHEMA,
                                            u.TABLE_NAME,
                                            u.COLUMN_NAME,
                                            u.ORDINAL_POSITION
                                     FROM   INFORMATION_SCHEMA.KEY_COLUMN_USAGE u
                                            INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                                                ON u.TABLE_SCHEMA = tc.CONSTRAINT_SCHEMA
                                                   AND u.TABLE_NAME = tc.TABLE_NAME
                                                   AND u.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                                     WHERE  CONSTRAINT_TYPE = 'PRIMARY KEY') pk
                        ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                           AND c.TABLE_NAME = pk.TABLE_NAME
                           AND c.COLUMN_NAME = pk.COLUMN_NAME
                    LEFT OUTER JOIN (SELECT DISTINCT
                                            u.TABLE_SCHEMA,
                                            u.TABLE_NAME,
                                            u.COLUMN_NAME
                                     FROM   INFORMATION_SCHEMA.KEY_COLUMN_USAGE u
                                            INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                                                ON u.TABLE_SCHEMA = tc.CONSTRAINT_SCHEMA
                                                   AND u.TABLE_NAME = tc.TABLE_NAME
                                                   AND u.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                                     WHERE  CONSTRAINT_TYPE = 'FOREIGN KEY') fk
                        ON c.TABLE_SCHEMA = fk.TABLE_SCHEMA
                           AND c.TABLE_NAME = fk.TABLE_NAME
                           AND c.COLUMN_NAME = fk.COLUMN_NAME
                    INNER JOIN INFORMATION_SCHEMA.TABLES t
                        ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
                           AND c.TABLE_NAME = t.TABLE_NAME
            LEFT OUTER JOIN (SELECT OBJECT_NAME(ep.major_id) AS [epTableName],
                    CAST(ep.Value AS nvarchar(500)) AS [epExtendedProperty]
                    FROM sys.extended_properties ep
                    WHERE ep.name = N'MS_Description' AND ep.minor_id = 0) As q
                ON t.table_name = q.epTableName
            LEFT OUTER JOIN (SELECT *, OBJECT_NAME(ep.major_id) AS [epTableName],
                    CAST(ep.Value AS nvarchar(500)) AS [epExtendedProperty]
                    FROM sys.extended_properties ep
                    WHERE ep.name = N'MS_Description') As qt
                ON c.ORDINAL_POSITION = qt.minor_id
                    AND  t.table_name = qt.epTableName
            WHERE c.TABLE_NAME NOT IN ('EdmMetadata', '__MigrationHistory')
            '''
        )

        self.documentation = cur.fetchall()
        cur.close()
        conn.close()

    def printDoc(self):
        out = []
        for row in self.documentation:
            for field in row:
                print(field, end="|")
                out.append(field)
            print(end="\n")
        return out
    # END Class


# print(query())

d = DB('sqlsrv')
d.server = 'chi-dpc01, 51664'
d.uid = 'doc'
d.catalog = 'DOCUMENTS'
d.pwd = 'whAt2up!'

d.getDocumentation()
d.printDoc()
