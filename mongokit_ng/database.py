from pymongo.database import Database as PymongoDB
from .collection import Collection
from .document import Document
from . import DBRef

class Database(PymongoDB):
    def __init__(self, *args, **kwargs):
        self._collections = dict()
        super().__init__(*args, **kwargs)

    def __getattr__(self, key):
        if key in self.connection._registered_documents:
            document = self.connection._registered_documents[key]
            return getattr(self[document.__collection__], key)
            
        if not key in self._collections:
            self._collections[key] = Collection(self, key)
        return self._collections[key]

    def __getitem__(self, key):
        return self.__getattr__(key)
        
    @property
    def connection(self):
        return self.client

    def dereference(self, dbref, model=None):
        if model is None:
            return super(Database, self).dereference(dbref)
        if not isinstance(dbref, DBRef):
            raise TypeError("first argument must be a DBRef")
        if dbref.database is not None and dbref.database != self.name:
            raise ValueError("trying to dereference a DBRef that points to "
                             "another database (%r not %r)" % (dbref.database, self._Database__name))
        if not issubclass(model, Document):
            raise TypeError("second argument must be a Document")
        return getattr(self[dbref.collection], model.__name__).one({'_id': dbref.id})