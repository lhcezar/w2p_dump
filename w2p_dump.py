try:
    from gluon.dal import DAL, Field
    import sys
except ImportError:
    pass


class DumpModels(object):
    sep = '\t'
    dal = 'db'

    def __init__(self, conn):
        try:
            self.db = db = \
                DAL(conn)
        except:
            print 'Falha de conexao'

    def generate(self, schema):
        catalog = CATALOG[self.db._adapter.dbengine](schema)

#        print catalog._read_tables()
        print catalog._information_schema()


class PostgresCatalog(DumpModels):
    tables = []
    # @todo: mapping each pgdatatype to w2p types
    # wherever unable therefore build the SQLCustomType
    TYPES = {
        'oid': 'integer',
        'bigint': 'integer',
        'timestamp with time zone': 'date',
        'name': 'text',
        'int4': 'integer',
        'character_data': 'text',
        'sql_identifier': 'text'
    }

    def __init__(self, schema):
        super(PostgresCatalog, self).__init__()
        self.schema = schema
        self._define_models()

    def _define_models(self):
        db = self.db
        self.pg_namespace = db.define_table('pg_namespace',
            Field('oid', 'id'),
            Field('nspname', 'text'),
            primarykey=['oid'],
            migrate=False
        )
        self.pg_class = db.define_table('pg_class',
            Field('oid', 'id'),
            Field('relname', 'text'),
            Field('relkind', 'text'),
            Field('relnamespace', self.pg_namespace),
            primarykey=['oid'],
            migrate=False
        )
        self.pg_type = db.define_table('pg_type',
            Field('oid', 'id'),
            Field('typname', 'text'),
            primarykey=['oid'],
            migrate=False
        )

        self.pg_attribute = db.define_table('pg_attribute',
            Field('attname', 'text'),
            Field('attnum', 'integer'),
            Field('attisdropped', 'boolean'),
            Field('attnotnull', 'boolean'),
            Field('atttypid', self.pg_type),
            Field('attrelid', self.pg_class),
            primarykey=['attname'],
            migrate=False
        )

    def _information_schema(self):
        db = self.db
        sql = "SET search_path TO information_schema, public, pg_catalog"
        db.executesql(sql)

        table = ''
        defaults = "%smigrate=False\n" % self.sep

        pg_type = self.pg_type
        pg_namespace = self.pg_namespace
        pg_class = self.pg_class
        pg_attribute = self.pg_attribute

        for relation in self._read_tables():
            table += "db.define_table('%s',\n" % relation.pg_class.relname.strip()
            for attribute in relation.pg_class.pg_attribute((pg_attribute.attnum > 0)
                             & (pg_attribute.atttypid == pg_type.oid)).select(pg_attribute.ALL, pg_type.ALL):

                try:
                    type = self.TYPES[attribute.pg_type.typname]
                except KeyError:
                    # @todo POC with SQLCustomType
                    type = attribute.pg_type.typname
                    required = 'required=required' if attribute.pg_attribute.attnotnull else ''

                table += "%sField('%s', '%s', '%s'),\n" % (self.sep, attribute.pg_attribute.attname, type, required)

            table += defaults
            table += ")\n"

        return table

    def _read_tables(self):
        db = self.db
        pg_class = self.pg_class
        pg_namespace = self.pg_namespace

        query_by_schema = pg_namespace.nspname.belongs(self.schema)
        query_by_relation = pg_class.relkind.belongs(('r', 'v'))

        relation = db((pg_class.relnamespace==pg_namespace.oid)
                            & (query_by_relation)
                            & (query_by_schema)
                            ).select(pg_class.ALL, pg_namespace.ALL)
        return relation



# has created a new adapter? update this dict with your new driver
CATALOG = {
    'postgres': PostgresCatalog
}

if __name__ == "__main__":
    str = "postgres://lhcezar:lhcezar@localhost:5432/postgres"
    c = DumpModels(str)
    schemas = 'information_schema',
    c.generate(schemas)
