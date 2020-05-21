from __future__ import print_function
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

import operator

long_type = int  # Python 3.x and up
try:
    long_type = long # Python 2.x
except NameError: pass

# "repeat_until" is a dynamically applied decorator that repeatedly calls the function
# given to it until that function's return value (or optionally a transform of it)
# satisfies the given condition.
#
# For example: If f(arg1,...,argN) returns some system variable and we're waiting for
# that variable's absolute value exceed 26, then the code fragment:
#
#   function_repeater = repeat_until (operator.gt , 26, transform = lambda x: abs(x)) (f)
#   function_repeater( arg1,...,argN )

# calls f the requested 'num_iter' times with sleeps of 'interval' seconds until this
# becomes true.  None is returned if the repetition times out before becoming true.

def repeat_until (op, value, interval=1.75, num_iter=28,
                           transform = lambda x:x,
                           debugPrinter = False):
    Err_pr = lambda *x,**kw: print(*x,**kw)
    Nul_pr = lambda *x,**kw: None
    if   debugPrinter is True:  pr = Err_pr
    elif debugPrinter is False: pr = Nul_pr
    else: pr = debugPrinter
    def deco(fn):
        def wrapper(*x,**kw):
            n_iter = num_iter
            while n_iter > 0:     # Do num_iter times:
                y = fn(*x,**kw)       # generate transform of
                y = transform(y)      #    fn(...) return value
                pr(y,end=' ')         # and
                if op(y,value):       # return True when/if it satisfies the comparison
                    pr('Y')
                    return True
                pr('N')
                n_iter -= 1           # Note if loop times out,
                sleep(interval)       #   we implicitly return None
        return wrapper
    return deco

def number_of_hits (json_result):
    return json.loads(json_result).get('hits',{}).get('total',0)

SOURCE_BOOKS_URL  = 'https://cdn.patricktriest.com/data/books.zip'
SOURCE_BOOKS_PATH = '/tmp/scratch'
SOURCE_BOOKS_SUBDIR  = 'books'

ELASTICSEARCH_PORT = 9100

DEFAULT_FULLTEXT_INDEX = 'full_text'
DEFAULT_METADATA_INDEX = 'metadata'


def get_source_books ( prefix_ = 'Books_', instance = None, attr = None ):

    Books_Path = tempfile.mkdtemp( prefix = prefix_ ) if (prefix_) else SOURCE_BOOKS_PATH
    if instance and attr: setattr(instance, attr, Books_Path)

    class BooksURLError(RuntimeError): pass
    class BooksDownloadError(RuntimeError): pass
    class BooksUnknownError(RuntimeError): pass

    basename = filter(any,SOURCE_BOOKS_URL.split("/"))[-1:]
    if ( not basename or not basename[0] or
         not basename[0].lower().endswith('.zip') ): raise BooksURLError
    basename = basename[0]

    if not os.path.isdir(Books_Path): os.mkdir(Books_Path)
    zipname = os.path.join(Books_Path,basename)
    if not os.path.exists(zipname):
        p = subprocess.Popen( 'wget -O '+ zipname + ' ' + SOURCE_BOOKS_URL,
             shell=True, stdout = subprocess.PIPE, stderr = subprocess.PIPE  )
        p.communicate()
        if p.returncode != 0: raise BooksDownloadError

    zipfile.ZipFile(zipname).extractall( path = Books_Path )
    retvalue = os.path.join(Books_Path, SOURCE_BOOKS_SUBDIR)
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


def search_index_for_avu_attribute_name(index_name, attr_name, port = ELASTICSEARCH_PORT):

    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' HTTP://localhost:{port}/{index_name}/text/_search?pretty=true -d '
        {{
            "from": 0, "size" : 500,
            "_source" : ["object_path", "attribute", "value", "units"],
            "query" : {{
                "term" : {{"attribute" : "{attr_name}"}}
            }}
        }}' """).format(**locals()))
    if rc != 0: out = None
    return out

def search_index_for_object_path(index_name, path_component, port = ELASTICSEARCH_PORT):

    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' HTTP://localhost:{port}/{index_name}/text/_search?pretty=true -d '
        {{
            "from": 0, "size" : 500,
            "_source" : ["object_path"],
            "query" : {{
                "term" : {{ "object_path" : "{path_component}"}}
            }}
        }}'""").format(**locals()))
    if rc != 0: out = None
    return out

def search_index_for_All_object_paths(index_name, port = ELASTICSEARCH_PORT):
    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' HTTP://localhost:{port}/{index_name}/text/_search?pretty=true -d '
        {{
            "from": 0, "size" : 500,
            "_source" : ["object_path", "data"],
            "query" : {{ "wildcard": {{ "object_path": {{
                "value": "*",
                "boost": 1.0,
                "rewrite": "constant_score" }} }} }} }}' """).format(**locals()))
    if rc != 0: out = None
    return out

def create_fulltext_index(index_name = DEFAULT_FULLTEXT_INDEX, port = ELASTICSEARCH_PORT):
    lib.execute_command("curl -X PUT -H'Content-Type: application/json' http://localhost:{port}/{index_name}".format(**locals()))
    lib.execute_command("curl -X PUT -H'Content-Type: application/json' http://localhost:{port}/{index_name}/_mapping/text ".format(**locals()) +
                        """ -d '{ "properties" : { "object_path" : { "type" : "text" }, "data" : { "type" : "text" } } }'""")

def create_metadata_index(index_name = DEFAULT_METADATA_INDEX, port = ELASTICSEARCH_PORT):
    lib.execute_command("curl -X PUT -H'Content-Type: application/json' http://localhost:{port}/{index_name}".format(**locals()))
    lib.execute_command("curl -X PUT -H'Content-Type: application/json' http://localhost:{port}/{index_name}/_mapping/text ".format(**locals()) +
                        """ -d '{ "properties" : { "object_path" : { "type" : "text" }, "attribute" : { "type" : "text" },"""\
                        """ "value" : { "type" : "text" }, "unit" : { "type" : "text" } } }'""")

def delete_fulltext_index(index_name = DEFAULT_FULLTEXT_INDEX, port = ELASTICSEARCH_PORT):
    lib.execute_command("curl -X DELETE -H'Content-Type: application/json' http://localhost:{port}/{index_name}".format(**locals()))

def delete_metadata_index(index_name = DEFAULT_METADATA_INDEX, port = ELASTICSEARCH_PORT):
    lib.execute_command("curl -X DELETE -H'Content-Type: application/json' http://localhost:{port}/{index_name}".format(**locals()))


debugFileName = None # -- or for example: "/tmp/debug.txt"  # -- for logging debug

class TestIndexingPlugin(ResourceBase, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.book_texts = get_source_books( instance = cls, attr = 'Books_Dir' )

    @classmethod
    def tearDownClass(cls):
        dir_to_delete = getattr( cls, 'Books_Dir', '' )
        if dir_to_delete:
            lib.execute_command(["rm","-fr",dir_to_delete])

    def setUp(self):
        super(TestIndexingPlugin, self).setUp()

    def tearDown(self):
        super(TestIndexingPlugin, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            self.remove_all_jobs_from_delay_queue( admin_session, initial_sleep=15.0 )

    @staticmethod
    def fail_unless_delay_queue_is_empty(session_):
        session_.assert_icommand("iqstat","STDOUT_SINGLELINE","No delayed rules")

    def remove_all_jobs_from_delay_queue(self, session_, initial_sleep=0.0):
        if initial_sleep > 0.0:
            sleep( initial_sleep )
        session_.assert_icommand('iqdel -a', 'STDERR')
        self.fail_unless_delay_queue_is_empty( session_ )

    def delay_queue_is_empty(self, session_, soft_assert = False, try_kill = False):
        if try_kill:
            out,_,rc = session_.run_icommand('iqdel -a')
            if isinstance(try_kill, (float,int,long_type)) and try_kill > 0.0: sleep(try_kill)
        out,_,rc = session_.run_icommand('iqstat')
        condition = (rc == 0 and 'No delayed rules' in out)
        if soft_assert:
            self.assertTrue( condition )
        return condition

    def test_response_to_elasticsearch_404_fulltext__issue_34(self):
        with indexing_plugin__installed():
            sleep(5)
            collection =  'fulltext_indexed_for_404_test'
            with session.make_session_for_existing_admin() as admin_session:
                self.remove_all_jobs_from_delay_queue(admin_session)
                admin_session.assert_icommand('imkdir -p {0}'.format(collection))
                f = tempfile.NamedTemporaryFile()
                f.write( "the quick brown fox"
                        ); f.flush();
                create_fulltext_index()
                try:
                    admin_session.assert_icommand("""imeta set -C {coll} """ \
                                                  """irods::indexing::index {ftidx}::full_text elasticsearch""".format(coll = collection,
                                                                                                                      ftidx = DEFAULT_FULLTEXT_INDEX))
                    self.assertEqual(0, number_of_hits(search_index_for_All_object_paths (DEFAULT_FULLTEXT_INDEX)))
                    admin_session.assert_icommand('iput {0} {1}'.format(f.name,collection))

                    data_object_path = collection + "/" + os.path.basename(f.name)
                    if debugFileName: debugPrt = lambda *a,**k: print(*a,file=open(debugFileName,'a'),**k)
                    else:             debugPrt = lambda *a,**k: None

                    # -- wait for data object to be indexed
                    self.assertIsNotNone (
                        repeat_until (operator.gt, 0, transform = number_of_hits, debugPrinter = debugPrt)
                                         (search_index_for_All_object_paths)
                                         (DEFAULT_FULLTEXT_INDEX)
                    )

                    # -- wait for queue to be clear
                    self.assertIsNotNone ( repeat_until (operator.eq, True) (self.delay_queue_is_empty) (admin_session) )

                    delete_fulltext_index()
                    sleep(5) ; debugPrt (">>>>")
                    admin_session.assert_icommand('irm -fr {data_object_path} '.format(**locals()))

                    self.assertIsNotNone ( repeat_until (operator.eq, True, transform = lambda x: 'object_purge' in x[0], debugPrinter = debugPrt )
                                             (lambda : admin_session.run_icommand('iqstat')) () )

                    debugPrt ("<<<< wait for object purge job()s to disappear")
                    self.assertIsNotNone ( repeat_until (operator.eq, False, transform = lambda x: 'object_purge' in x[0], debugPrinter = debugPrt )
                                             (lambda : admin_session.run_icommand('iqstat')) () )
                finally:
                    admin_session.assert_icommand('irm -fr {collection}'.format(**locals()))
                    delete_fulltext_index()

    def test_response_to_elasticsearch_404_metadata__issue_34(self):
        with indexing_plugin__installed():
            sleep(5)
            collection =  'AVUs_indexed_for_404_test'
            with session.make_session_for_existing_admin() as admin_session:
                self.remove_all_jobs_from_delay_queue(admin_session)
                admin_session.assert_icommand('imkdir -p {0}'.format(collection))
                f = tempfile.NamedTemporaryFile()
                create_metadata_index()
                try:
                    admin_session.assert_icommand("""imeta set -C {coll} """ \
                                                  """irods::indexing::index {mdidx}::metadata elasticsearch""".format(coll = collection,
                                                                                                                      mdidx = DEFAULT_METADATA_INDEX))
                    admin_session.assert_icommand('iput {0} {1}'.format(f.name,collection))
                    data_object_path = collection + "/" + os.path.basename(f.name)
                    admin_session.assert_icommand('imeta set -d {data_object_path} atr0 val0'.format(**locals()))

                    if debugFileName: debugPrt = lambda *a,**k: print(*a,file=open(debugFileName,'a'),**k)
                    else:             debugPrt = lambda *a,**k: None

                    self.assertIsNotNone (
                        repeat_until (operator.eq, 1, transform = number_of_hits, debugPrinter = debugPrt)
                                          (search_index_for_avu_attribute_name)
                                          (DEFAULT_METADATA_INDEX, 'atr0')
                    )
                    delete_metadata_index()
                    sleep(5) ; debugPrt (">>>>")
                    admin_session.assert_icommand('imeta rm -d {data_object_path} atr0 val0'.format(**locals()))

                    self.assertIsNotNone ( repeat_until (operator.eq, True, transform = lambda x: 'metadata_purge' in x[1], debugPrinter = debugPrt)
                                                            (lambda : admin_session.assert_icommand("iqstat", "STDOUT" )) () )
                    debugPrt ("<<<< wait for purge jobs to disappear")
                    self.assertIsNotNone ( repeat_until (operator.eq, False, transform = lambda x: 'metadata_purge' in x[1] or 'metadata_index' in x[1],
                                                         debugPrinter = debugPrt)
                                                             ( lambda : admin_session.assert_icommand("iqstat", "STDOUT" )) () )
                finally:
                    admin_session.assert_icommand('irm -fr {collection}'.format(**locals()))
                    admin_session.assert_icommand("iadmin rum") # remove unused metadata
                    delete_metadata_index()

    def test_indexing_01_basic(self):
        with indexing_plugin__installed():
            sleep(5)
            test_collections = set()
            with session.make_session_for_existing_admin() as admin_session:
                self.remove_all_jobs_from_delay_queue(admin_session)
                n = 0
                attr_j = 0
                keylist = dict()
                indexed_collections =  {
                    'full_text' : ['fulltext_test_coll'],
                    'metadata'  : ['metadata_test_coll'],
                }
                try:
                    create_metadata_index ('metadata_index')
                    create_fulltext_index ('full_text_index')
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

                    # wait for indexing jobs to clear delayed rule queue
                    self.assertIsNotNone ( repeat_until (operator.eq, True ,interval=1.75, num_iter=50) (self.delay_queue_is_empty) (admin_session) )

                    for (key, search_strings) in keylist.items():
                        apply_criterion = search_index_for_object_path if key.startswith('full_text') \
                                     else search_index_for_avu_attribute_name
                        for target in search_strings:
                            json_result = apply_criterion(key + "_index", target)
                            self.assertTrue( json.loads(json_result).get('hits',{}).get('total',0) >= 1 ,
                                             "Didn't find matches for target '{target}' in index '{key}_index' ".format(**locals()) )
                finally:
                    for collection in test_collections:
                        admin_session.assert_icommand("""irm -fr {c}""".format(c = collection))
                    # - repeat test-and-wait for delay queue to clear
                    repeat_until (operator.eq, True, interval=0.1, num_iter=12) (self.delay_queue_is_empty) (admin_session, try_kill = 10.0)
                    delete_metadata_index ('metadata_index')
                    delete_fulltext_index ('full_text_index')
