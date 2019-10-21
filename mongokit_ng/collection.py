import random
import warnings

from pymongo.collection import Collection as PymongoCollection
from .mongo_exceptions import MultipleResultsFound
from .cursor import Cursor

class Collection(PymongoCollection):
    def __init__(self, *args, **kwargs):
        self._documents = {}
        self._collections = {}
        super().__init__(*args, **kwargs)
        self._registered_documents = self.database.client._registered_documents

    def __getattr__(self, key):
        if key in self._registered_documents:
            if not key in self._documents:
                self._documents[key] = self._registered_documents[key](collection=self)
                if hasattr(self._documents[key], "i18n") and self._documents[key].i18n:
                    # It seems that if we want i18n, we have to call twice the constructor.
                    # Why on earth ? I don't know and I don't have the time to investigate yet.
                    self._documents[key]()
                if self._documents[key].indexes:
                    warnings.warn('%s: Be careful, index generation is not automatic anymore.'
                         'You have to generate your index youself' % self._documents[key]._obj_class.__name__,
                         DeprecationWarning)
                #self._documents[key].generate_index(self)
            return self._documents[key]
        else:
            newkey = "%s.%s" % (self.name, key)
            if not newkey in self._collections:
                self._collections[newkey] = Collection(self.database, newkey)
            return self._collections[newkey]

    def __getitem__(self, key):
        return self.__getattr__(key)

    def __call__(self, *args, **kwargs):
        if "." not in self.__name:
            raise TypeError("'Collection' object is not callable. If you "
                            "meant to call the '%s' method on a 'Database' "
                            "object it is failing because no such method "
                            "exists." %
                            self.__name)
        name = self.__name.split(".")[-1]
        raise TypeError("'Collection' object is not callable. "
                        "If you meant to call the '%s' method on a 'Collection' "
                        "object it is failing because no such method exists.\n"
                        "If '%s' is a Document then you may have forgotten to "
                        "register it to the connection." % (name, name))

    def find(self, *args, **kwargs):
        return Cursor(self, *args, **kwargs)
    
    def find_and_modify(self, *args, **kwargs):
        obj_class = kwargs.pop('wrap', None)
        doc = super().find_and_modify(*args, **kwargs)
        if doc and obj_class:
            return self.collection[obj_class.__name__](doc)
        return doc

    def get_from_id(self, id):
        return self.find_one({"_id": id})


    def one(self, *args, **kwargs):
        bson_obj = self.find(*args, **kwargs)
        count = bson_obj.count()
        if count > 1:
            raise MultipleResultsFound("%s results found" % count)
        elif count == 1:
            return next(bson_obj)

    def find_random(self):
        max = self.count()
        if max:
            num = random.randint(0, max-1)
            return next(self.find().skip(num))
    
    def find_fulltext(self, search, **kwargs):
        return self.database.command("text", self.name, search=search, **kwargs)
