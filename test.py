# -*- coding: utf-8 -*-
# Copyright (c) 2023, Silvio Peroni <essepuntato@gmail.com>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.
import unittest
from os import sep
from pandas import DataFrame
from impl import JournalUploadHandler, CategoryUploadHandler
from impl import JournalQueryHandler, CategoryQueryHandler
from impl import FullQueryEngine
from impl import Journal, Category, Area

# REMEMBER: before launching the tests, please run the Blazegraph instance!

class TestProjectBasic(unittest.TestCase):

    # The paths of the files used in the test should change depending on what you want to use
    # and the folder where they are. Instead, for the graph database, the URL to talk with
    # the SPARQL endpoint must be updated depending on how you launch it - currently, it is
    # specified the URL introduced during the course, which is the one used for a standard
    # launch of the database.
    journal = "test_data" + sep + "doaj.csv"
    category = "test_data" + sep + "scimago.json"
    relational = "." + sep + "trial.db"
    graph = " http://10.201.3.82:9999/blazegraph/sparql"
    
    def test_01_JournalUploadHandler(self):
        # print('1_entrato_test_01_JournalUploadHandler')
        u = JournalUploadHandler()
        self.assertTrue(u.setDbPathOrUrl(self.graph))
        # print('setDbPathOrUrl')
        self.assertEqual(u.getDbPathOrUrl(), self.graph)
        # print('getDbPathOrUrl')
        self.assertTrue(u.pushDataToDb(self.journal))
        # print('pushDataToDb')
        # print('2_finito_test_01_JournalUploadHandler')

    def test_02_CategoryUploadHandler(self):
        # print('1_entrato_test_02_CategoryUploadHandler')
        u = CategoryUploadHandler()
        self.assertTrue(u.setDbPathOrUrl(self.relational))  
        self.assertEqual(u.getDbPathOrUrl(), self.relational)
        # print('pushDataToDb')
        self.assertTrue(u.pushDataToDb(self.category))
        # print('pushDataToDb')
        # print('2_finito_test_02_CategoryUploadHandler')
        
    
    def test_03_JournalQueryHandler(self):
        # print('1_entrato_test_03_JournalQueryHandler')
        q = JournalQueryHandler()
        self.assertTrue(q.setDbPathOrUrl(self.graph))
        self.assertEqual(q.getDbPathOrUrl(), self.graph)
        # print('trying_getById')
        self.assertIsInstance(q.getById("just_a_test"), DataFrame)
        # print('ending_getById')
        # print('trying_getAllJournals')
        self.assertIsInstance(q.getAllJournals(), DataFrame)
        # print('ending_getAllJournals')
        # print('trying_getJournalsWithTitle')
        self.assertIsInstance(q.getJournalsWithTitle("just_a_test"), DataFrame)
        # print('ending_getJournalsWithTitle')
        # print('trying_getJournalsPublishedBy')
        self.assertIsInstance(q.getJournalsPublishedBy("just_a_test"), DataFrame)
        # print('ending_getJournalsPublishedBy')
        # print('trying_getJournalsWithLicense')
        self.assertIsInstance(q.getJournalsWithLicense({"just_a_test"}), DataFrame)
        # print('ending_getJournalsWithLicense')
        # print('trying_getJournalsWithAPC')
        self.assertIsInstance(q.getJournalsWithAPC(), DataFrame)
        # print('ending_getJournalsWithAPC')
        # print('trying_getJournalsWithDOAJSeal')
        self.assertIsInstance(q.getJournalsWithDOAJSeal(), DataFrame)
        # print('ending_getJournalsWithDOAJSeal')
        # print('2_finito_test_03_JournalQueryHandler')

    
    def test_04_ProcessDataQueryHandler(self):
        # print('1_entrato_test_04_ProcessDataQueryHandler')
        q = CategoryQueryHandler()
        self.assertTrue(q.setDbPathOrUrl(self.relational))
        self.assertEqual(q.getDbPathOrUrl(), self.relational)

        self.assertIsInstance(q.getById("just_a_test"), DataFrame)

        self.assertIsInstance(q.getAllCategories(), DataFrame)
        self.assertIsInstance(q.getAllAreas(), DataFrame)
        self.assertIsInstance(q.getCategoriesWithQuartile({"just_a_test"}), DataFrame)
        self.assertIsInstance(q.getCategoriesAssignedToAreas({"just_a_test"}), DataFrame)
        self.assertIsInstance(q.getAreasAssignedToCategories({"just_a_test"}), DataFrame)
        # print('2_finito_test_04_ProcessDataQueryHandler')
        
    def test_05_FullQueryEngine(self):
        # print('1_entrato_test_05_FullQueryEngine')
        jq = JournalQueryHandler()
        jq.setDbPathOrUrl(self.graph)
        cq = CategoryQueryHandler()
        cq.setDbPathOrUrl(self.relational)

        fq = FullQueryEngine()
        self.assertIsInstance(fq.cleanJournalHandlers(), bool)
        self.assertIsInstance(fq.cleanCategoryHandlers(), bool)
        self.assertTrue(fq.addJournalHandler(jq))
        self.assertTrue(fq.addCategoryHandler(cq))
        # print('controllo_metodi_full_query')
        self.assertEqual(fq.getEntityById("just_a_test"), None)
        # print('2_finito_')

        r = fq.getAllJournals()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Journal)
        # print('2_finito_')

        r = fq.getJournalsWithTitle("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Journal)
        # print('2_finito_')

        r = fq.getJournalsPublishedBy("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Journal)
        # print('2_finito_')

        r = fq.getJournalsWithLicense({"just_a_test"})
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Journal)
        # print('2_finito_')

        r = fq.getJournalsWithAPC()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Journal)
        # print('2_finito_')

        r = fq.getJournalsWithDOAJSeal()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Journal)
        # print('2_finito_')

        r = fq.getAllCategories()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Category)
        # print('2_finito_')

        r = fq.getAllAreas()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Area)
        # print('2_finito_')

        r = fq.getCategoriesWithQuartile({"just_a_test"})
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Category)
        # print('2_finito_')

        r = fq.getCategoriesAssignedToAreas({"just_a_test"})
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Category)
        # print('2_finito_')

        r = fq.getAreasAssignedToCategories({"just_a_test"})
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Area)
        # print('2_finito_')

        r = fq.getJournalsInCategoriesWithQuartile({"just_a_test"}, {"just_a_test"})
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Journal)
        # print('2_finito_')

        r = fq.getJournalsInAreasWithLicense({"just_a_test"}, {"just_a_test"})
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Journal)
        # print('2_finito_')

        r = fq.getDiamondJournalsInAreasAndCategoriesWithQuartile({"just_a_test"}, {"just_a_test"}, {"just_a_test"})
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Journal) 
        # print('2_finito_')

