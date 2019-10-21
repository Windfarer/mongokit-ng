#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2009-2011, Nicolas Clairon
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the University of California, Berkeley nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import unittest

from mongokit_ng.schema_document import *
from mongokit_ng import Document, Connection

class TypesTestCase(unittest.TestCase):

    def setUp(self):
        self.connection = Connection()
        self.col = self.connection.test.mongokit

    def tearDown(self):
        self.connection.drop_database('test')


    def test_authorized_type(self):
       for auth_type in SchemaDocument.authorized_types:
            if auth_type is dict:
                auth_type = {}
            class MyDoc(SchemaDocument):
                structure = { "foo":auth_type }
            if type(auth_type) is dict:
                assert MyDoc() == {"foo":{}}, MyDoc()
            elif auth_type is list:
                assert MyDoc() == {"foo":[]}
            else:
                assert MyDoc() == {"foo":None}, auth_type

    def test_not_authorized_type(self):
        for unauth_type in [set]:
            failed = False
            try:
                class MyDoc(SchemaDocument):
                    structure = { "foo":[unauth_type] }
            except StructureError as e:
                self.assertEqual(str(e), "MyDoc: %s is not an authorized type" % unauth_type)
                failed = True
            self.assertEqual(failed, True)
            failed = False
            try:
                class MyDoc(SchemaDocument):
                    structure = { "foo":(unauth_type) }
            except StructureError as e:
                self.assertEqual(str(e), "MyDoc: %s is not an authorized type" % unauth_type)
                failed = True
            self.assertEqual(failed, True)
            failed = False
            try:
                class MyDoc2(SchemaDocument):
                    structure = { 'foo':[{int:unauth_type }]}
            except StructureError as e:
                self.assertEqual(str(e), "MyDoc2: %s is not an authorized type" % unauth_type)
                failed = True
            self.assertEqual(failed, True)
            failed = False
            try:
                class MyDoc3(SchemaDocument):
                    structure = { 'foo':[{unauth_type:int }]}
            except AuthorizedTypeError as e:
                self.assertEqual(str(e), "MyDoc3: %s is not an authorized type" % unauth_type)
                failed = True
            self.assertEqual(failed, True)

        failed = False
        try:
            class MyDoc4(SchemaDocument):
                structure = {1:str}
        except StructureError as e:
            self.assertEqual(str(e), "MyDoc4: 1 must be a str or a type")
            failed = True
        self.assertEqual(failed, True)


    def test_type_from_functions(self):
        from datetime import datetime
        class MyDoc(SchemaDocument):
            structure = {
                "foo":datetime,
            }
        assert MyDoc() == {"foo":None}, MyDoc()
        mydoc = MyDoc()
        mydoc['foo'] = datetime.now()
        mydoc.validate()

    def test_non_typed_list(self):
        class MyDoc(SchemaDocument):
            structure = {
                "foo":[]
            }
        mydoc = MyDoc()
        mydoc.validate()
        assert mydoc['foo'] == []
        mydoc['foo'] = ["bla", 23]
        mydoc.validate()
        mydoc['foo'] = [set([1,2]), "bla"]
        self.assertRaises(AuthorizedTypeError, mydoc.validate)
        mydoc['foo'] = "bla"
        self.assertRaises(SchemaTypeError, mydoc.validate)

#        class MyDoc(SchemaDocument):
#            structure = {
#                "foo":list
#            }
#        mydoc = MyDoc()
#        mydoc.validate()
#        assert mydoc['foo'] == []
#        mydoc['foo'] = [u"bla", 23]
#        mydoc.validate()
#        mydoc['foo'] = [set([1,2]), "bla"]
#        self.assertRaises(AuthorizedTypeError, mydoc.validate)

    def test_typed_list(self):
        class MyDoc(SchemaDocument):
            structure = {
                "foo":[int]
            }
        mydoc = MyDoc()
        mydoc.validate()
        assert mydoc['foo'] == []
        mydoc['foo'] = [1,2,3]
        mydoc.validate()
        mydoc['foo'] = ["bla"]
        self.assertRaises(SchemaTypeError, mydoc.validate)

    def test_typed_list_with_dict(self):
        class MyDoc(SchemaDocument):
            structure = {
                "foo":[{str:int}]
            }
        mydoc = MyDoc()
        mydoc['foo'] = [{"bla":1},{"ble":2}]
        mydoc.validate()
        mydoc['foo'] = [{"bla":"bar"}]
        self.assertRaises(SchemaTypeError, mydoc.validate)

    def test_typed_list_with_list(self):
        class MyDoc(SchemaDocument):
            structure = {
                "foo":[[str]]
            }
        mydoc = MyDoc()
        mydoc['foo'] = [["bla","blu"],["ble","bli"]]
        mydoc.validate()
        mydoc['foo'] = [["bla",1]]
        self.assertRaises(SchemaTypeError, mydoc.validate)

    def test_typed_tuple(self):
        class MyDoc(SchemaDocument):
            structure = {
                "foo":(int, str, float)
            }
        mydoc = MyDoc()
        mydoc.validate()
        assert mydoc['foo'] == [None, None, None]
        mydoc['foo'] = ["bla", 1, 4.0]
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = [1, "bla"]
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = "bla"
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = [1,'bar',3.2]
        mydoc.validate()
        mydoc['foo'] = [None, "bla", 3.1]
        mydoc.validate()
        mydoc['foo'][0] = 50
        mydoc.validate()

    def test_nested_typed_tuple(self):
        class MyDoc(SchemaDocument):
            structure = {
                "foo":{'bar':(int, str, float)}
            }
        mydoc = MyDoc()
        mydoc.validate()
        assert mydoc['foo']['bar'] == [None, None, None]
        mydoc['foo']['bar'] = ["bla", 1, 4.0]
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo']['bar'] = [1, "bla"]
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo']['bar'] = [1,'bar',3.2]
        mydoc.validate()
        mydoc['foo']['bar'] = [None, "bla", 3.1]
        mydoc.validate()
        mydoc['foo']['bar'][0] = 50
        mydoc.validate()

    def test_saving_tuple(self):
        class MyDoc(Document):
            structure = { 'foo': (int, str, float) }
        self.connection.register([MyDoc])

        mydoc = self.col.MyDoc()
        assert mydoc == {'foo': [None, None, None]}, mydoc
        mydoc['foo'] = (1, 'a', 1.1) # note that this will be converted to list
        assert mydoc == {'foo': (1, 'a', 1.1000000000000001)}, mydoc
        mydoc.save()
        mydoc = self.col.find_one()

        class MyDoc(Document):
            structure = {'foo':[str]}
        self.connection.register([])
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        mydoc['foo'] = ('bla', 'bli', 'blu', 'bly')
        mydoc.save()
        mydoc = self.col.get_from_id(mydoc['_id'])


    def test_nested_typed_tuple_in_list(self):
        class MyDoc(SchemaDocument):
            structure = {
                "foo":{'bar':[(int, str, float)]}
            }
        mydoc = MyDoc()
        mydoc.validate()
        assert mydoc == {'foo': {'bar': []}}
        mydoc['foo']['bar'].append(["bla", 1, 4.0])
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo']['bar'] = []
        mydoc['foo']['bar'].append([1, "bla"])
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo']['bar'] = []
        mydoc['foo']['bar'].append([1,'bar',3.2])
        mydoc.validate()
        mydoc['foo']['bar'].append([None, "bla", 3.1])
        mydoc.validate()
        mydoc['foo']['bar'][1][0] = 50
        mydoc.validate()

    def test_dict_str_typed_list(self):
        class MyDoc(SchemaDocument):
            structure = {
                "foo":{str:[int]}
            }
        mydoc = MyDoc()
        mydoc['foo'] = {"bar":[1,2,3]}
        mydoc.validate()
        mydoc['foo'] = {"bar":["bla"]}
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = {3:[1,2,3]}
        self.assertRaises(SchemaTypeError, mydoc.validate)

    def test_with_custom_object(self):
        class MyDict(dict):
            pass
        class MyDoc(SchemaDocument):
            structure = {
                "foo":{str:int}
            }
        mydoc = MyDoc()
        mydict = MyDict()
        mydict["foo"] = 3
        mydoc["foo"] = mydict
        mydoc.validate()

    def test_custom_object_as_type(self):
        class MyDict(dict):
            pass
        class MyDoc(SchemaDocument):
            structure = {
                "foo":MyDict({str:int})
            }
        mydoc = MyDoc()
        mydict = MyDict()
        mydict["foo"] = 3
        mydoc["foo"] = mydict
        mydoc.validate()
        mydoc['foo'] = {"foo":"7"}
        self.assertRaises(SchemaTypeError, mydoc.validate)

        class MyInt(int):
            pass
        class MyDoc(SchemaDocument):
            structure = {
                "foo":MyInt,
            }
        mydoc = MyDoc()
        mydoc["foo"] = MyInt(3)
        mydoc.validate()
        mydoc['foo'] = 3
        self.assertRaises(SchemaTypeError, mydoc.validate)

    def test_list_instead_of_dict(self):
        class MyDoc(SchemaDocument):
            structure = {
                "foo":{str:[str]}
            }
        mydoc = MyDoc()
        mydoc['foo'] = ['bla']
        self.assertRaises(SchemaTypeError, mydoc.validate)

    def _test_big_nested_example(self):
        # XXX TODO
        class MyDoc(SchemaDocument):
            structure = {
                "foo":{str:[int], "bar":{"spam":{int:[str]}}},
                "bla":{"blo":{"bli":[{"arf":str}]}},
            }
        mydoc = MyDoc()
        mydoc['foo'].update({"bir":[1,2,3]})
        mydoc['foo']['bar']['spam'] = {1:['bla', 'ble'], 3:['foo', 'bar']}
        mydoc.validate()
        mydoc['bla']['blo']['bli'] = [{"bar":["bla"]}]
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['bla']['blo']['bli'] = [{"arf":[1]}]
        self.assertRaises(SchemaTypeError, mydoc.validate)


    def test_adding_custom_type(self):
        class MyDoc(SchemaDocument):
            structure = {
                "foo":str,
            }
            authorized_types = SchemaDocument.authorized_types + [str]
        mydoc = MyDoc()

    def test_schema_operator(self):
        from mongokit_ng.operators import SchemaOperator
        class OP(SchemaOperator):
            repr = "op"
        op = OP()
        self.assertRaises(NotImplementedError, op.validate, "bla")


    def test_or_operator(self):
        from mongokit_ng import OR
        assert repr(OR(int, str)) == "<int or str>"

        failed = False

        from datetime import datetime
        class MyDoc(SchemaDocument):
            structure = {
                "foo":OR(str,int),
                "bar":OR(str, datetime),
                "foobar": OR(str, int),
            }

        mydoc = MyDoc()
        assert str(mydoc.structure['foo']) == '<str or int>'
        assert str(mydoc.structure['bar']) == '<str or datetime>'
        assert str(mydoc.structure['foobar']) == '<str or int>'
        assert mydoc == {'foo': None, 'bar': None, 'foobar': None}
        mydoc['foo'] = 3.0
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = "foo"
        mydoc.validate()
        mydoc['foo'] = 3
        mydoc.validate()
        mydoc['foo'] = datetime.now()
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = "foo"
        mydoc['bar'] = datetime.now()
        mydoc.validate()
        mydoc['bar'] = "today"
        mydoc.validate()
        mydoc['bar'] = 25
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['bar'] = "bar"
        mydoc["foo"] = "foo"
        mydoc["foobar"] = "foobar"
        mydoc.validate()
        mydoc["foobar"] = datetime.now()
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc["foobar"] = 3
        mydoc.validate()

    def test_not_operator(self):
        from mongokit_ng import NOT
        failed = False

        from datetime import datetime
        class MyDoc(SchemaDocument):
            structure = {
                "foo":NOT(str,int),
                "bar":NOT(datetime),
                "foobar": NOT(str)
            }

        mydoc = MyDoc()
        assert str(mydoc.structure['foo']) == '<not str, not int>', str(mydoc.structure['foo'])
        assert str(mydoc.structure['bar']) == '<not datetime>'
        assert str(mydoc.structure['foobar']) == '<not str>'
        assert mydoc == {'foo': None, 'bar': None, 'foobar': None}
        assert mydoc['foo'] is None
        assert mydoc['bar'] is None
        assert mydoc['foobar'] is None
        mydoc['foo'] = 3
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = "foo"
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = 3.0
        mydoc.validate()
        mydoc['foo'] = datetime.now()
        mydoc.validate()

        mydoc['bar'] = datetime.now()
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['bar'] = "today"
        mydoc.validate()
        mydoc['bar'] = 25
        mydoc.validate()
        mydoc['foobar'] = 'abc'
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foobar'] = 1
        mydoc.validate()

    def test_is_operator(self):
        from mongokit_ng import IS
        failed = False

        from datetime import datetime
        class MyDoc(SchemaDocument):
            structure = {
                "foo":IS('spam','eggs'),
                "bar":IS('3', 3)
            }

        mydoc = MyDoc()
        assert str(mydoc.structure['foo']) == "<is 'spam' or is 'eggs'>"
        assert str(mydoc.structure['bar']) == "<is '3' or is 3>"
        assert mydoc == {'foo': None, 'bar': None}
        assert mydoc['foo'] is None
        assert mydoc['bar'] is None
        mydoc['foo'] = 3
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = "bla"
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = datetime.now()
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = "spam"
        mydoc.validate()
        mydoc['foo'] = "eggs"
        mydoc.validate()

        mydoc['bar'] = datetime.now()
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['bar'] = "today"
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['bar'] = 'foo'
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['bar'] = 3
        mydoc.validate()
        mydoc['bar'] = "3"
        mydoc.validate()

    def test_subclassed_type(self):
        """
        accept all subclass of supported type
        """
        class CustomFloat(float):
            def __init__(self, float):
                self = float + 2
        class MyDoc(SchemaDocument):
            structure = {
                "foo":float,
            }
        mydoc = MyDoc()
        mydoc['foo'] = CustomFloat(4)
        mydoc.validate()


    def test_set_type(self):
        from mongokit_ng import Set
        class MyDoc(Document):
            structure = {
                "tags":Set(int),
            }

        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        mydoc['tags'] = set(["1","1","2","3","4"])
        self.assertRaises(ValueError, mydoc.validate)
        mydoc['tags'] = set([1,1,2,3,4])
        mydoc.save()

        doc = self.col.MyDoc.find_one()
        assert doc['tags'] == set([1,2,3,4]), doc['tags']

    def test_set_type2(self):
        class MyDoc(Document):
                structure = {
                        'title':str,
                        'category':Set(str)
                }
                required_fields=['title']
        self.connection.register([MyDoc])
        doc = self.col.MyDoc()
        print(doc) # {'category': set([]), 'title': None}
        assert isinstance(doc['category'], set)
        try:
                doc.validate()
        except RequireFieldError as e:
                print(e) # title is required

        print(doc) # {'category': [], 'title': None}
        assert isinstance(doc['category'], set)
        doc['title']='hello'
        doc.validate()

    def test_int_type(self):
        @self.connection.register
        class MyDoc(Document):
            structure = {
                "foo":int,
            }

        mydoc = self.col.MyDoc()
        mydoc['foo'] = ''
        self.assertRaises(SchemaTypeError, mydoc.validate)
        mydoc['foo'] = 10
        mydoc.save()

    def test_uuid_type(self):
        import uuid
        @self.connection.register
        class MyDoc(Document):
            structure = {
                'uuid': uuid.UUID,
            }
        uid = uuid.uuid4()
        obj = self.col.MyDoc()
        obj['uuid'] = uid
        obj.save()

        assert isinstance(self.col.MyDoc.find_one()['uuid'], uuid.UUID)

    def test_binary_with_str_type(self):
        import bson
        @self.connection.register
        class MyDoc(Document):
            structure = {
                'my_binary': str,
            }
        obj = self.col.MyDoc()
        # non-utf8 string
        non_utf8 = b"\xFF\xFE\xFF";
        obj['my_binary'] = non_utf8

        self.assertRaises(SchemaTypeError, obj.validate)

    def test_binary_with_binary_type(self):
        import bson
        @self.connection.register
        class MyDoc(Document):
            structure = {
                'my_binary': bytes,
            }
        obj = self.col.MyDoc()
        # non-utf8 string
        non_utf8 = b"\xFF\xFE\xFF";
        obj['my_binary'] = non_utf8
        obj.save()

        self.assertEqual(self.col.MyDoc.find_one()['my_binary'], non_utf8)
