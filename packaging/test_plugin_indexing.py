import os
import sys
import shutil
import contextlib
import tempfile
import json
import os.path

import zipfile
import subprocess
from time import sleep
from textwrap import dedent

if sys.version_info >= (2, 7):
    import unittest
else:
    import unittest2 as unittest

from ..configuration import IrodsConfig
from ..controller import IrodsController
from .resource_suite import ResourceBase
from ..test.command import assert_command
from . import session
from .. import test
from .. import paths
from .. import lib
import ustrings

SOURCE_BOOKS_URL  = 'https://cdn.patricktriest.com/data/books.zip'
SOURCE_BOOKS_PATH = '/tmp/scratch'
SOURCE_BOOKS_SUBDIR  = 'books'

def get_source_books ():
    class BooksURLError(RuntimeError): pass
    class BooksDownloadError(RuntimeError): pass
    class BooksUnknownError(RuntimeError): pass
    basename = filter(any,SOURCE_BOOKS_URL.split("/"))[-1:]
    if ( not basename or not basename[0] or
         not basename[0].lower().endswith('.zip') ): raise BooksURLError
    basename = basename[0]
    if not os.path.isdir(SOURCE_BOOKS_PATH): os.mkdir(SOURCE_BOOKS_PATH)
    zipname = os.path.join(SOURCE_BOOKS_PATH,basename)
    if not os.path.exists(zipname):
        p = subprocess.Popen( 'wget -O '+ zipname + ' ' + SOURCE_BOOKS_URL,
             shell=True, stdout = subprocess.PIPE, stderr = subprocess.PIPE  )
        p.communicate()
        if p.returncode != 0: raise BooksDownloadError
    zipfile.ZipFile(zipname).extractall(path=SOURCE_BOOKS_PATH)
    retvalue = os.path.join(SOURCE_BOOKS_PATH,SOURCE_BOOKS_SUBDIR)
    if not os.path.isdir(retvalue): raise BooksUnknownError
    return retvalue

@contextlib.contextmanager
def indexing_plugin__installed(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['rule_engine_server_sleep_time_in_seconds'] = 5
        irods_config.server_config['plugin_configuration']['rule_engines'][:0] = [
            {
                "instance_name": "irods_rule_engine_plugin-indexing-instance",
                "plugin_name": "irods_rule_engine_plugin-indexing",
                "plugin_specific_configuration": {
                }
            },
            {
                "instance_name": "irods_rule_engine_plugin-elasticsearch-instance",
                "plugin_name": "irods_rule_engine_plugin-elasticsearch",
                "plugin_specific_configuration": {
                    "hosts" : ["http://localhost:9100/"],
                    "bulk_count" : 100,
                    "read_size" : 4194304
                }
            },
            {
                "instance_name": "irods_rule_engine_plugin-document_type-instance",
                "plugin_name": "irods_rule_engine_plugin-document_type",
                "plugin_specific_configuration": {
                }
            }
        ]
        irods_config.commit(irods_config.server_config, irods_config.server_config_path)
        IrodsController().restart()
        try:
            yield
        finally:
            pass


def search_index_for_avu_attribute_name( index_name , attr_name ):
    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' HTTP://localhost:9100/{index_name}/text/_search?pretty=true -d '
        {{
            "from": 0, "size" : 500,
            "_source" : ["object_path", "attribute", "value", "units"],
            "query" : {{
                "term" : {{"attribute" : "{attr_name}"}}
            }}
        }}' """).format(**locals()))
    if rc != 0: out = None
    return out

def search_index_for_object_path( index_name , path_component ):
    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' HTTP://localhost:9100/{index_name}/text/_search?pretty=true -d '
        {{
            "from": 0, "size" : 500,
            "_source" : ["object_path"],
            "query" : {{
                "term" : {{ "object_path" : "{path_component}"}}
            }}
        }}'""").format(**locals()))
    if rc != 0: out = None
    return out


class TestIndexingPlugin(ResourceBase, unittest.TestCase):

    def setUp(self):
        super(TestIndexingPlugin, self).setUp()
        self.book_texts = get_source_books()

    def tearDown(self):
        super(TestIndexingPlugin, self).tearDown()

    def test_indexing_01_basic(self):
        with indexing_plugin__installed():
            sleep(5)
            test_collections = set()
            with session.make_session_for_existing_admin() as admin_session:
                n = 0
                attr_j = 0
                keylist = dict()
                indexed_collections =  {
                    'full_text' : ['fulltext_test_coll'],
                    'metadata'  : ['metadata_test_coll'],
                }
                try:
                    ## Create index for each type
                    lib.execute_command("""curl -X PUT -H'Content-Type: application/json' http://localhost:9100/full_text_index""")
                    lib.execute_command("""curl -X PUT -H'Content-Type: application/json' http://localhost:9100/full_text_index/_mapping/text """\
                                        """-d '{ "properties" : { "object_path" : { "type" : "text" }, "data" : { "type" : "text" } } }'""");
                    lib.execute_command("""curl -X PUT -H'Content-Type: application/json' http://localhost:9100/metadata_index""")
                    lib.execute_command("""curl -X PUT -H'Content-Type: application/json' http://localhost:9100/metadata_index/_mapping/text """\
                                        """-d '{ "properties" : { "object_path" : { "type" : "text" }, "attribute" : { "type" : "text" },"""\
                                        """ "value" : { "type" : "text" }, "unit" : { "type" : "text" } } }'""")
                    for (key, collection_list) in indexed_collections.items():
                        keylist.setdefault( key, [] )
                        for collection in collection_list:
                            admin_session.assert_icommand('imkdir -p {0}'.format(collection))
                            add_or_set = ("add" if collection in test_collections else "set")
                            admin_session.assert_icommand("""imeta {add_or_set} -C {collection} """ \
                                                          """irods::indexing::index {key}_index::{key} elasticsearch""".format(**locals()))
                            test_collections |= set((collection,))
                            n += 1
                            _,_,rc = admin_session.run_icommand('iput -r {books} {c}/books{i:03}'.format( books=self.book_texts, c=collection, i=n))
                            self.assertTrue(0 == rc, 'recursive iput of books directory')
                            if key == 'metadata':  # ... pick out data object and assign it an AVU
                                out,_,rc=admin_session.run_icommand('''iquest --no-page '%s/%s' "select COLL_NAME,DATA_NAME where COLL_NAME like'''\
                                                                    ''' '%/{c}/books{i:03}%'"'''.format(c=collection,i=n))
                                self.assertTrue(rc == 0,'iquest failed')
                                data_obj_name = filter(any,map(lambda s:s.strip(),out.split('\n')))[0]
                                attr_j += 1
                                admin_session.assert_icommand("""imeta set -d {d} attr{j} val0 units0""".format(d = data_obj_name, j=attr_j))
                                keylist[key] += [ "attr{j}".format(j=attr_j) ]
                            else:
                                keylist[key] += [ "books{i:03}".format(i=n) ]
                    sleep(30)  # combined interval to wait for irods delay queue to be serviced and indexing to be done
                    for (key, search_strings) in keylist.items():
                        apply_criterion = search_index_for_object_path if key.startswith('full_text') \
                                     else search_index_for_avu_attribute_name
                        for target in search_strings:
                            json_result = apply_criterion(key + "_index", target)
                            self.assertTrue( json.loads(json_result).get('hits',{}).get('total',0) >= 1 ,
                                             "Didn't find matches for target '{target}' in index '{key}_index' ".format(**locals()) )
                finally:
                    # Delete index for each type
                    lib.execute_command("""curl -X DELETE -H'Content-Type: application/json' http://localhost:9100/full_text_index""")
                    lib.execute_command("""curl -X DELETE -H'Content-Type: application/json' http://localhost:9100/metadata_index""")
                    for collection in test_collections:
                        admin_session.assert_icommand("""irm -fr {c}""".format(c = collection))
