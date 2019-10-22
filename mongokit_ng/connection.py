from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.read_preferences import ReadPreference
from .database import Database
from collections.abc import Iterable
import warnings
class CallableMixin(object):
    def __call__(self, doc=None, gen_skel=True, lang='en', fallback_lang='en'):
        return self._obj_class(
            doc=doc,
            gen_skel=gen_skel,
            collection=self.collection,
            lang=lang,
            fallback_lang=fallback_lang
        )

class MongoPieConnection:
    def __init__(self, *args, **kwargs):
        self._databases = dict()
        self._registered_documents = dict()
        super().__init__(*args, **kwargs)

    def register(self, obj_list):
        decorator = None
        if not isinstance(obj_list, Iterable):
            decorator = obj_list
            obj_list = [obj_list]

        for _, db in list(self._databases.items()):
            for __, col in list(db._collections.items()):
                for doc_name, ___ in list(col._documents.items()):
                    del col._documents[doc_name]
                for obj_name in [obj.__name__ for obj in obj_list]:
                    if obj_name in col._registered_documents:
                        del col._registered_documents[obj_name]

        for obj in obj_list:
            CallableDocument = type(
                "Callable%s" % obj.__name__,
                (obj, CallableMixin),
                {"_obj_class": obj, "__repr__": object.__repr__}
            )
            self._registered_documents[obj.__name__] = CallableDocument
        
        if decorator is not None:
            return decorator

    def __getattr__(self, key):
        document = self._registered_documents.get(key)
        if document is not None:
            try:
                return getattr(self[document.__database__][document.__collection__], key)
            except AttributeError:
                raise AttributeError("%s: __collection__ attribute not found. "
                                     "You cannot specify the `__database__` attribute without "
                                     "the `__collection__` attribute" % key)
        if key not in self._databases:
            self._databases[key] = Database(self, key)
        return self._databases[key]

    def __getitem__(self, key):
        return self.__getattr__(key)
        
class Connection(MongoPieConnection, MongoClient):
    def __init__(self, *args, **kwargs):
        kwargs["connect"]= False

        # remove pymongo3 unsupported arg
        # ref https://api.mongodb.com/python/current/migrate-to-pymongo3.html
        if "safe" in kwargs:
            del kwargs["safe"]

        # fixme: new HA https://api.mongodb.com/python/current/examples/high_availability.html
        if "secondary_acceptable_latency_ms" in kwargs:
            warnings.warn("DeprecationWarning: secondary_acceptable_latency_ms is deprecated.")
            ms = kwargs["secondary_acceptable_latency_ms"]
            del kwargs["secondary_acceptable_latency_ms"]
            if "readPreference" not in kwargs:
                kwargs["readPreference"]= ReadPreference.NEAREST
            kwargs["localThresholdMS"]=ms
    
        super().__init__(*args, **kwargs)

        # check connected
        self.admin.command("ismaster")

def del_args(key, kwargs):
    if key in kwargs:
        del kwargs[key]