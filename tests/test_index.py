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

from mongokit_ng import Connection, Document, OperationFailure, BadIndexError, INDEX_GEO2D, INDEX_ASCENDING, INDEX_DESCENDING

class IndexTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection()
        self.col = self.connection.test.mongokit

    def tearDown(self):
        self.connection['test'].drop_collection('mongokit')
        self.connection = None

    def test_index_basic(self):
        class Movie(Document):
            structure = {
                'standard':str,
                'other':{
                    'deep':str,
                },
                'notindexed':str,
            }

            indexes = [
                {
                    'fields':['standard','other.deep'],
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        movie = self.col.Movie()
        self.col.Movie.generate_index(self.col.Movie.collection)
        movie['standard'] = 'test'
        movie['other']['deep'] = 'testdeep'
        movie['notindexed'] = 'notthere'
        movie.save()

        db = self.connection.test
        idx_info = self.col.index_information()
        assert 'standard_1_other.deep_1' in idx_info and idx_info['standard_1_other.deep_1'].get('unique'), 'No Index Found'

        movie = self.col.Movie()
        movie['standard'] = 'test'
        movie['other']['deep'] = 'testdeep'
        self.assertRaises(OperationFailure, movie.save)

    def test_index_single_without_generation(self):
        class Movie(Document):
            structure = {
                'standard':str,
            }

            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        movie = self.col.Movie()
        movie['standard'] = 'test'
        movie.save()

        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.mongokit', 'name':'standard_1', 'unique':True, 'key':{'standard':1}})

        assert item is None, 'Index is found'

    def test_index_single(self):
        class Movie(Document):
            structure = {
                'standard':str,
            }

            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_index(self.col.Movie.collection)
        movie = self.col.Movie()
        movie['standard'] = 'test'
        movie.save()

        db = self.connection.test
        idx_info = self.col.index_information()
        assert 'standard_1' in idx_info, 'No Index Found'
        idx = idx_info['standard_1']
        assert idx.get('unique'), idx
        assert idx['key'] == [('standard',1)]

    def test_index_multi(self):
        class Movie(Document):
            structure = {
                'standard':str,
                'other':{
                    'deep':str,
                },
                'notindexed':str,
                'alsoindexed':str,
            }

            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
                {
                    'fields':['alsoindexed', 'other.deep'],
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_index(self.col.Movie.collection)
        movie = self.col.Movie()
        movie['standard'] = 'test'
        movie.save()

        db = self.connection.test
        idx_info = self.col.index_information()
        assert 'standard_1' in idx_info, 'No Index Found'
        assert 'alsoindexed_1_other.deep_1' in idx_info, 'No Index Found'
        idx = idx_info['standard_1']
        idx2 = idx_info['alsoindexed_1_other.deep_1']
        assert idx.get('unique'), idx
        assert idx2.get('unique'), idx2
        assert idx['key'] == [('standard',1)]

        movie = self.col.Movie()
        movie['standard'] = 'test'
        self.assertRaises(OperationFailure, movie.save)

    def test_index_multi2(self):
        class Movie(Document):
            structure = {
                'standard':str,
                'other':{
                    'deep':str,
                },
                'notindexed':str,
                'alsoindexed':str,
            }

            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
                {
                    'fields':['other.deep'],
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_index(self.col.Movie.collection)
        movie = self.col.Movie()
        movie['standard'] = 'test'
        movie['other']['deep'] = 'foo'
        movie.save()

        db = self.connection.test

        idx_info = self.col.index_information()
        assert 'standard_1' in idx_info, 'No Index Found'
        assert 'other.deep_1' in idx_info, 'No Index Found'
        idx = idx_info['standard_1']
        idx2 = idx_info['other.deep_1']
        assert idx.get('unique')
        assert idx2.get('unique')
        assert idx['key'] == [('standard',1)], idx['key']

        movie = self.col.Movie()
        movie['standard'] = 'test'
        self.assertRaises(OperationFailure, movie.save)

        movie = self.col.Movie()
        movie['other']['deep'] = 'foo'
        self.assertRaises(OperationFailure, movie.save)

    def test_index_direction(self):
        class Movie(Document):
            structure = {
                'standard':str,
                'other':{
                    'deep':str,
                },
                'notindexed':str,
                'alsoindexed':str,
            }

            indexes = [
                {
                    'fields':('standard',INDEX_DESCENDING),
                    'unique':True,
                },
                {
                    'fields':[('alsoindexed',INDEX_ASCENDING), ('other.deep',INDEX_DESCENDING)],
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_index(self.col.Movie.collection)
        movie = self.col.Movie()
        movie['standard'] = 'test'
        movie.save()

        db = self.connection.test

        idx_info = self.col.index_information()
        assert 'standard_-1' in idx_info, 'No Index Found'
        assert 'alsoindexed_1_other.deep_-1' in idx_info, 'No Index Found'
        idx = idx_info['standard_-1']
        idx2 = idx_info['alsoindexed_1_other.deep_-1']
        assert idx.get('unique')
        assert idx2.get('unique')

    def test_index_direction_GEO2D(self):
        class Movie(Document):
            structure = {
                'standard':str,
                'other':{
                    'deep':str,
                },
                'notindexed':str,
                'alsoindexed':str,
            }

            indexes = [
                {
                    'fields':('standard',INDEX_GEO2D),
                    'unique':True,
                },
                {
                    'fields':[('alsoindexed',INDEX_GEO2D), ('other.deep',INDEX_DESCENDING)],
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_index(self.col.Movie.collection)
        movie = self.col.Movie()
        movie['standard'] = 'test'
        movie.save()

        idx_info = self.col.index_information()
        assert 'standard_2d' in idx_info, 'No Index Found'
        assert 'alsoindexed_2d_other.deep_-1' in idx_info, 'No Index Found'
        idx = idx_info['standard_2d']
        idx2 = idx_info['alsoindexed_2d_other.deep_-1']
        assert idx.get('unique'), idx
        assert idx2.get('unique'), idx2

    def test_bad_index_descriptor(self):
        failed = False
        try:
            class Movie(Document):
                structure = {'standard':str}
                indexes = [{'unique':True}]
        except BadIndexError as e:
            self.assertEqual(str(e), "'fields' key must be specify in indexes")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                structure = {
                    'standard':str,
                }
                indexes = [
                    {
                        'fields':('standard',INDEX_DESCENDING),
                        'uniq':True,
                    },
                ]
        except BadIndexError as e:
            self.assertEqual(str(e), "uniq is unknown key for indexes")
            failed = True
        #self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                structure = {
                    'standard':str,
                }
                indexes = [
                    {
                        'fields':'std',
                    },
                ]
        except ValueError as e:
            self.assertEqual(str(e), "Error in indexes: can't find std in structure")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                structure = {
                    'standard':str,
                }
                indexes = [
                    {
                        'fields':{'standard':1},
                    },
                ]
        except BadIndexError as e:
            self.assertEqual(str(e), "fields must be a string, a tuple or a list of tuple (got <class 'dict'> instead)")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                structure = {
                    'standard':str,
                }
                indexes = [
                    {
                        'fields':('standard',1, "blah"),
                    },
                ]
        except BadIndexError as e:
            self.assertEqual(str(e), "Error in indexes: a tuple must contain only two value : the field name and the direction")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                structure = {
                    'standard':str,
                }
                indexes = [
                    {
                        'fields':('standard',"2"),
                    },
                ]
        except BadIndexError as e:
            self.assertEqual(str(e), "index direction must be INDEX_DESCENDING, INDEX_ASCENDING, INDEX_OFF, INDEX_ALL, INDEX_GEO2D, INDEX_GEOHAYSTACK, or INDEX_GEOSPHERE. Got 2")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                structure = {
                    'standard':str,
                }
                indexes = [
                    {
                        'fields':(3,1),
                    },
                ]
        except BadIndexError as e:
            self.assertEqual(str(e), "Error in 3, the field name must be string (got <class 'int'> instead)")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                structure = {
                    'standard':str,
                }
                indexes = [
                    {
                        'fields':("blah",1),
                    },
                ]
        except ValueError as e:
            self.assertEqual(str(e), "Error in indexes: can't find blah in structure")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                structure = {
                    'standard':str,
                }
                indexes = [
                    {
                        'fields':[('standard',1), ('bla',1)],
                    },
                ]
        except ValueError as e:
            self.assertEqual(str(e), "Error in indexes: can't find bla in structure")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                structure = {
                    'standard':str,
                }
                indexes = [
                    {
                        'fields':[('standard',3)],
                    },
                ]
        except BadIndexError as e:
            self.assertEqual(str(e), "index direction must be INDEX_DESCENDING, INDEX_ASCENDING, INDEX_OFF, INDEX_ALL, INDEX_GEO2D, INDEX_GEOHAYSTACK, or INDEX_GEOSPHERE. Got 3")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                structure = {
                    'standard':str,
                }
                indexes = [
                    {
                        'fields':['std'],
                    },
                ]
        except ValueError as e:
            self.assertEqual(str(e), "Error in indexes: can't find std in structure")
            failed = True
        self.assertEqual(failed, True)

    def test_index_ttl(self):
        class Movie(Document):
            structure = {
                'standard':str,
            }

            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                    'ttl': 86400
                },
        # If indexes are still broken validation will choke on the ttl
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_index(self.col)
        movie = self.col.Movie()
        movie['standard'] = 'test'
        movie.save()

        idx_info = self.col.index_information()
        assert 'standard_1' in idx_info, 'No Index Found'
        idx = idx_info['standard_1']
        assert idx.get('unique'), idx
        assert idx['key'] == [('standard',1)]

    def test_index_simple_inheritance(self):
        class DocA(Document):
            structure = {
                'standard':str,
            }

            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
            ]

        class DocB(DocA):
            structure = {
                'docb':str,
            }

        self.connection.register([DocA, DocB])
        self.col.DocB.generate_index(self.col)
        docb = self.col.DocB()
        docb['standard'] = 'test'
        docb['docb'] = 'foo'
        docb.save()

        db = self.connection.test
        idx_info = self.col.index_information()
        assert 'standard_1' in idx_info, 'No Index Found'
        idx = idx_info['standard_1']
        assert idx.get('unique')
        assert idx['key'] == [('standard',1)], idx['key']

    def test_index_inheritance(self):
        class DocA(Document):
            structure = {
                'standard':str,
            }

            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
            ]

        class DocB(DocA):
            structure = {
                'docb':str,
            }
            indexes = [
                {
                    'fields':'docb',
                    'unique':True,
                },
            ]
        self.connection.register([DocA, DocB])
        self.col.DocB.generate_index(self.col.DocB.collection)


        docb = self.col.DocB()
        docb['standard'] = 'test'
        docb['docb'] = 'foo'
        docb.save()

        idx_info = self.col.index_information()
        assert 'standard_1' in idx_info, 'No Index Found'
        assert 'docb_1' in idx_info, 'No Index Found'
        idx = idx_info['standard_1']
        idx2 = idx_info['docb_1']
        assert idx.get('unique'), idx
        assert idx2.get('unique'), idx2
        assert idx['key'] == [('standard',1)]
        assert idx2['key'] == [('docb',1)]

    def test_index_real_world(self):
        import datetime
        class MyDoc(Document):
            structure = {
                "mydoc":{
                    "creation_date":datetime.datetime,
                }
            }
            indexes = [{'fields':[('mydoc.creation_date',-1), ('_id',1)]}]
        self.connection.register([MyDoc])

        date = datetime.datetime.utcnow()

        mydoc = self.col.MyDoc()
        mydoc['mydoc']['creation_date'] = date
        mydoc['_id'] = 'aaa'
        mydoc.save()


        mydoc3 = self.col.MyDoc()
        mydoc3['mydoc']['creation_date'] = date
        mydoc3['_id'] = 'bbb'
        mydoc3.save()

        import time
        time.sleep(1)
        date2 = datetime.datetime.utcnow()

        mydoc2 = self.col.MyDoc()
        mydoc2['mydoc']['creation_date'] = date2
        mydoc2['_id'] = 'aa'
        mydoc2.save()

        time.sleep(1)
        date3 = datetime.datetime.utcnow()

        mydoc4 = self.col.MyDoc()
        mydoc4['mydoc']['creation_date'] = date3
        mydoc4['_id'] = 'ccc'
        mydoc4.save()

        self.col.ensure_index([('mydoc.creation_date',-1), ('_id',1)])
        results = [i['_id'] for i in self.col.MyDoc.fetch().sort([('mydoc.creation_date',-1),('_id',1)])]
        assert results == ['ccc', 'aa', 'aaa', 'bbb'], results

    def test_index_pymongo(self):
        import datetime
        date = datetime.datetime.utcnow()
        import pymongo
        collection = pymongo.MongoClient()['test']['test_index']

        mydoc = {'mydoc':{'creation_date':date}, '_id':'aaa'}
        collection.insert(mydoc)

        mydoc2 = {'mydoc':{'creation_date':date}, '_id':'bbb'}
        collection.insert(mydoc2)

        import time
        time.sleep(1)
        date2 = datetime.datetime.utcnow()

        mydoc3 = {'mydoc':{'creation_date':date2}, '_id':'aa'}
        collection.insert(mydoc3)

        time.sleep(1)
        date3 = datetime.datetime.utcnow()

        mydoc4 = {'mydoc':{'creation_date':date3}, '_id':'ccc'}
        collection.insert(mydoc4)

        collection.ensure_index([('mydoc.creation_date',-1), ('_id',1)])
        #print list(collection.database.system.indexes.find())

        results = [i['_id'] for i in collection.find().sort([('mydoc.creation_date',-1),('_id',1)])]
        print(results)
        assert results  == ['ccc', 'aa', 'aaa', 'bbb'], results

    def test_index_inheritance2(self):
        class A(Document):
            structure = {
                'a':{
                    'title':str,
                }
            }
            indexes = [{'fields':'a.title'}]

        class B(A):
            structure = {
                'b':{
                    'title':str,
                }
            }
            indexes = [{'fields':'b.title'}]


        class C(Document):
            structure = {
                'c':{
                    'title':str,
                }
            }
            indexes = [{'fields':'c.title'}]

        class D(B, C):
            structure = {
                'd':{
                    'title':str,
                }
            }

        self.connection.register([D])
        doc = self.col.D()
        assert doc.indexes == [{'fields': 'b.title'}, {'fields': 'a.title'}, {'fields': 'c.title'}]

    def test_index_with_default_direction(self):
        class MyDoc(Document):
            structure = {
                'foo': str,
                'bar': int
            }
            indexes = [
                {'fields': ['foo', ('bar', -1)]},
            ]
        self.connection.register([MyDoc])
        self.col.MyDoc.generate_index(self.col)
        for i in range(10):
           doc = self.col.MyDoc()
           doc['foo'] = str(i)
           doc['bar'] = i
           doc.save()
        idx_info = self.col.index_information()
        assert 'foo_1_bar_-1' in idx_info

    def test_index_with_check(self):
        @self.connection.register
        class MyDoc(Document):
            structure = {
                'foo': dict,
                'bar': int
            }
            indexes = [
                    {'fields': ['foo.title'], 'check':False},
            ]
        self.col.MyDoc.generate_index(self.col)
        for i in range(10):
           doc = self.col.MyDoc()
           doc['foo']['title'] = str(i)
           doc['bar'] = i
           doc.save()
        idx_info = self.col.index_information()
        assert 'foo.title_1' in idx_info

    def test_index_with_check_is_true(self):
        @self.connection.register
        class MyDoc(Document):
            structure = {
                'foo': str,
                'bar': int
            }
            indexes = [
                    {'fields': ['foo'], 'check':True},
            ]
        self.col.MyDoc.generate_index(self.col)
        for i in range(10):
           doc = self.col.MyDoc()
           doc['foo'] = str(i)
           doc['bar'] = i
           doc.save()
        idx_info = self.col.index_information()
        assert 'foo_1' in idx_info

    def test_index_with_additional_keywords(self):
        @self.connection.register
        class KWDoc(Document):
            structure = {
                'foo': str,
            }
            indexes = [
                {
                    'fields':[
                        "foo"
                    ],
                    'dropDups':True,
                    'name':'additional_kws',
                }
            ]
        self.col.KWDoc.generate_index(self.col)
        idx_info = self.col.index_information()
        assert 'additional_kws' in idx_info
        idx = idx_info.get('additional_kws')
        assert idx.get("dropDups"), idx_info
