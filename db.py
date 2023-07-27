import pyodbc
import settings


class DBConnection:
    cnxn: pyodbc.Connection = None

    def get_connection(self) -> pyodbc.Connection:
        if self.cnxn:
            return self.cnxn

        server = f"{settings.DATABASES['bidf']['HOST']},{settings.DATABASES['bidf']['PORT']}"
        database = settings.DATABASES['bidf']['NAME']
        username = settings.DATABASES['bidf']['USER']
        password = settings.DATABASES['bidf']['PASSWORD']
        driver = settings.DATABASES['bidf']['OPTIONS']['driver']

        self.cnxn = pyodbc.connect(
            'DRIVER={' + driver + '};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
        )
        return self.cnxn

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DBConnection, cls).__new__(cls)
        return cls.instance
