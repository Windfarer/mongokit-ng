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

from mongokit_ng import *
from bson.objectid import ObjectId
from gridfs import NoFile


class GridFSTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection()
        self.col = self.connection.test.mongokit

    def tearDown(self):
        self.connection.drop_database('test')

    def test_simple_gridfs(self):
        class Doc(Document):
            structure = {
                'title':str,
            }
            gridfs = {'files': ['source']}
        self.connection.register([Doc])
        doc = self.col.Doc()
        doc['title'] = 'Hello'
        doc.save()

        assertion = False
        try:
            assert doc.fs.source is None
        except NoFile:
            assertion = True
        assert assertion

        assertion = False
        try:
            print(doc.fs.not_a_file)
        except AttributeError:
            assertion = True
        assert assertion
        doc.fs.source = "Hello World !"
        assert doc.fs.source == b"Hello World !", doc.fs.source

        doc = self.col.Doc.find_one({'title':'Hello'})
        assert doc.fs.source == b"Hello World !"

        f = doc.fs.get_last_version('source')
        assert f.name == 'source'

        del doc.fs.source

        assertion = False
        try:
            assert doc.fs.source is None
        except NoFile:
            assertion = True
        assert assertion

        doc.fs.source = "bla"
        assert [i.name for i in doc.fs] == ['source'], [i.name for i in doc.fs]

    def test_gridfs_without_saving(self):
        class Doc(Document):
            structure = {
                'title':str,
            }
            gridfs = {'files': ['source']}
        self.connection.register([Doc])
        doc = self.col.Doc()
        doc['title'] = 'Hello'
        assertion = False
        try:
            doc.fs.source = "Hello World !"
        except RuntimeError:
            assertion = True
        assert assertion
        doc.save()
        doc.fs.source = 'Hello world !'

    def test_gridfs_bad_type(self):
        class Doc(Document):
            structure = {
                'title':str,
            }
            gridfs = {'files': ['source']}
        self.connection.register([Doc])
        doc = self.col.Doc()
        doc['title'] = 'Hello'
        doc.save()
        assertion = False
        try:
            doc.fs.source = 3
        except TypeError:
            assertion = True
        assert assertion
        assertion = False

    def test_gridfs_with_container(self):
        class Doc(Document):
            structure = {
                'title':str,
            }
            gridfs = {
                'files': ['source'],
                'containers': ['images']
            }

        self.connection.register([Doc])
        doc = self.col.Doc()
        doc['title'] = 'Hello'
        doc.save()

        doc.fs.source = "Hello World !"
        assert doc.fs.source == b"Hello World !", doc.fs.source

        assertion = False
        try:
            doc.fs.images['first.jpg'] = 3
        except TypeError:
            assertion = True
        assert assertion

        doc.fs.images['first.jpg'] = "My first image"
        doc.fs.images['second.jpg'] = "My second image"

        assert doc.fs.images['first.jpg'] == b'My first image', doc.fs.images['first.jpg']
        assert doc.fs.images['second.jpg'] == b'My second image'

        doc.fs.images['first.jpg'] = "My very first image"
        assert doc.fs.images['first.jpg'] == b'My very first image', doc.fs.images['first.jpg']

        del doc.fs.images['first.jpg']

        assertion = False
        try:
            doc.fs.images['first.jpg']
        except NoFile:
            assertion = True
        assert assertion

    def test_gridfs_list(self):
        class Doc(Document):
            structure = {
                'title':str,
            }
            gridfs = {'files': ['foo', 'bla'], 'containers':['attachments']}
        self.connection.register([Doc])
        doc = self.col.Doc()
        doc['title'] = 'Hello'
        doc.save()

        doc.fs.foo = "Hello World !"
        doc.fs.bla = "Salut !"
        assert [i.name for i in doc.fs] == ['foo', 'bla'], [i.name for i in doc.fs]
        doc.fs.attachments['eggs.txt'] = "Ola !"
        doc.fs.attachments['spam.txt'] = "Saluton !"
        assert [(i.container, i.name) for i in doc.fs.attachments] == [('attachments', 'eggs.txt'), ('attachments', 'spam.txt')], [(i.container, i.name) for i in doc.fs.attachments]
        assert [i.name for i in doc.fs] == ['foo', 'bla', 'eggs.txt', 'spam.txt'], [(i.container, i.name) for i in doc.fs]


    def test_gridfs_new_file(self):
        class Doc(Document):
            structure = {
                'title':str,
            }
            gridfs = {'files': ['foo', 'bla'], 'containers':['attachments']}
        self.connection.register([Doc])
        doc = self.col.Doc()
        doc['title'] = 'Hello'
        doc.save()

        doc.fs.foo = "Hello World !"
        f = doc.fs.new_file("bla")
        f.write('Salut !')
        f.close()
        assert doc.fs.bla == b"Salut !", doc.fs.bla
        assert doc.fs.foo == b"Hello World !", doc.fs.foo

        f = doc.fs.attachments.new_file('test')
        f.write('this is a test')
        f.close()
        assert doc.fs.attachments['test'] == b'this is a test', doc.fs.attachments['test']

        doc = self.col.Doc.find_one()
        assert doc.fs.bla == b"Salut !", doc.fs.bla
        assert doc.fs.foo == b"Hello World !", doc.fs.foo 
        assert doc.fs.attachments['test'] == b'this is a test', doc.fs.attachments['test']
        assert doc.fs.attachments.get_last_version('test').read() == b'this is a test', doc.fs.attachments.get_last_version('test').read()


    def test_pymongo_compatibility(self):
        class Doc(Document):
            structure = {
                'title':str,
            }
            gridfs = {'files': ['source', 'foo'], 'containers':['attachments']}
        self.connection.register([Doc])
        doc = self.col.Doc()
        doc['title'] = 'Hello'
        doc.save()
        id = doc.fs.put("Hello World", filename="source")
        assert doc.fs.get(id).read() == b'Hello World', doc.fs.get(id).read()
        assert doc.fs.get_last_version("source").name == 'source', doc.fs.get_last_version("source").name
        assert doc.fs.get_last_version("source").read() == b'Hello World', doc.fs.get_last_version("source").read()
        f = doc.fs.new_file("source")
        f.write("New Hello World!")
        f.close()
        assert doc.fs.source == b'New Hello World!', doc.fs.source
        new_id = doc.fs.get_last_version("source")._id
        doc.fs.delete(new_id)
        assert doc.fs.source == b'Hello World', doc.fs.source

