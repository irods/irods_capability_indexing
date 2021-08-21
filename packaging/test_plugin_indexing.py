from __future__ import print_function
import os
import sys
import shutil
import contextlib
import tempfile
import json
import re
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

# -----------

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

def repeat_until (op, value, interval=1.75, num_iter=30,   # Reasonable defaults for indexing tests.
                           transform = lambda x:x,         # Identity (no transform) by default.
                           debugPrinter = False,           # True for printing to stdout or define own.
                           threshold = 1):                 # Require this many consecutive true results.
    threshold = abs(int(threshold))
    if threshold == 1: threshold = 0  ## don't count trivial case of one consecutive true result
    Err_pr = lambda *x,**kw: print(*x,**kw)
    Nul_pr = lambda *x,**kw: None
    if   debugPrinter is True:  pr = Err_pr
    elif debugPrinter is False: pr = Nul_pr
    else: pr = debugPrinter
    def deco(fn):
        def wrapper(*x,**kw):
            n_iter = num_iter
            while n_iter != 0:     # Do num_iter times:
                y = fn(*x,**kw)       # generate transform of
                y = transform(y)      #    fn(...) return value
                pr(y,end=' ')         # and
                if op(y,value):       # return True when/if it satisfies the comparison
                    pr('Y',n_iter)
                    if threshold == 0: # if zero threshold, report success immediately
                        return True
                    else:              # else require [threshold] consecutive true results
                        if n_iter <= -threshold: return True
                        if n_iter > 0: n_iter = -1
                else:
                    pr('N',n_iter)
                    if n_iter < 0 : break
                n_iter -= 1
                sleep(interval)
            # end while : we've timed out. Implicitly return None
        return wrapper
    return deco


# `number_of_hits' can be used as the transform keyword argument in `repeat_until(...)'.
# Thus if a function F (index_name, A,V,U) returns a json formatted string j indicating an
# integer number of hits under the 'hits' key, we can do the following:
#
# >>> repeater_for_F = repeat_until(operator.gt, N, transform = number_of_hits) (F)
# >>> result = repeater_for_F (index_name,A,V,U)
#
# after which `result' contains either the number of hits on the first iteration of F satisfying
# the condition, or None in case of timeout.

def _DEFAULT_EXTRACTOR(struct_):
    return struct_.get('hits',{}).get('total',0)

def number_of_hits (json_string_result, action_hook = None, n_hits_extractor = _DEFAULT_EXTRACTOR):
    struct = json.loads(json_string_result)
    if callable(action_hook):
        action_hook(struct)   # e.g. store the deserialized JSON
    return n_hits_extractor(struct)


# `make_number_of_hits_fcn(d)' returns a function wrapping a call to `number_of_hits', with the side
# effect of setting the first element of `d' to the deserialized JSON structure last returned from F.
# Any further elements of `d' are erased. Example use:
#
#   >>> d = []
#   >>> rep_result = repeat_until(operator.ge, 1, transform=number_of_hits_fn(d), ...) (F) (index_name,A,V,U)
#   >>> if rep_result is not None: print(d[0], 'was the qualifying result')
#
# This has the same effect as with the `number_of_hits' example given above, except that the most recent
# JSON returned by F will be placed (in deserialized form) into d[0].  Note d[0] only qualifies as
# "satisfying the waited-for condition" if rep_result is not None.


def make_number_of_hits_fcn(storage_list = None, extractor = _DEFAULT_EXTRACTOR):
    def insert(result):
        storage_list[:] = [result]
    func = insert if isinstance(storage_list,list) else lambda _:None
    return lambda js: number_of_hits(js, action_hook = func, n_hits_extractor = extractor)

# -----------

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

ES_VERSION = '7.x'
def es7_exactly (): return '8.' > ES_VERSION >= '7.'
def es7_or_later(): return ES_VERSION >= '7.'

@contextlib.contextmanager
def indexing_plugin__installed(indexing_config=()):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['rule_engine_server_sleep_time_in_seconds'] = 5

        indexing_plugin_specific_config = {} # hard-wired configuration entries could go here
        indexing_plugin_specific_config.update( indexing_config )

        irods_config.server_config['plugin_configuration']['rule_engines'][:0] = [
            {
                "instance_name": "irods_rule_engine_plugin-indexing-instance",
                "plugin_name": "irods_rule_engine_plugin-indexing",
                "plugin_specific_configuration": indexing_plugin_specific_config
            },
            {
                "instance_name": "irods_rule_engine_plugin-elasticsearch-instance",
                "plugin_name": "irods_rule_engine_plugin-elasticsearch",
                "plugin_specific_configuration": {
                    "hosts" : ["http://localhost:{}/".format(ELASTICSEARCH_PORT)],
                    "bulk_count" : 100,
                    "read_size" : 4194304,
                    "es_version" : ES_VERSION
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


# Assuming use for metadata style of index only

def search_index_for_avu_attribute_name(index_name, attr_name, port = ELASTICSEARCH_PORT):
    maptype = "" if es7_or_later() else "/text"
    track_num_hits_as_int = "&track_total_hits=true&rest_total_hits_as_int=true" if es7_exactly() else ""
    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' HTTP://localhost:{port}/{index_name}{maptype}/_search?pretty=true{track_num_hits_as_int} -d '
        {{
            "from": 0, "size" : 500,
            "_source" : ["absolutePath", "metadataEntries"],
            "query" : {{
                "nested": {{
                  "path": "metadataEntries",
                  "query": {{
                    "bool": {{
                      "must": [
                        {{ "match": {{ "metadataEntries.attribute": "{attr_name}" }} }}
                      ]
                    }}
                  }}
                }}
            }}
        }}' """).format(**locals()))
    if rc != 0: out = None
    return out


# Assuming use for fulltext style of index only

def search_index_for_object_path(index_name, path_component, extra_source_fields="", port = ELASTICSEARCH_PORT):
    path_component_matcher = ("*/" + path_component + ("/*" if not path_component.endswith("$") else "")).rstrip("$")
    track_num_hits_as_int = "&track_total_hits=true&rest_total_hits_as_int=true" if es7_exactly() else ""
    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' HTTP://localhost:{port}/{index_name}/text/_search?pretty=true{track_num_hits_as_int} -d '
        {{
            "from": 0, "size" : 500,
            "_source" : ["absolutePath" {extra_source_fields} ],
            "query" : {{
                "wildcard" : {{
                    "absolutePath" : {{
                        "value" :  "*/{path_component_matcher}"
                    }}
                }}
            }}
        }}'""").format(**locals()))
    if rc != 0: out = None
    return out


# Assuming use for fulltext style of index only

def search_index_for_All_object_paths(index_name, port = ELASTICSEARCH_PORT):
    track_num_hits_as_int = "&track_total_hits=true&rest_total_hits_as_int=true" if es7_exactly() else ""
    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' HTTP://localhost:{port}/{index_name}/text/_search?pretty=true{track_num_hits_as_int} -d '
        {{
            "from": 0, "size" : 500,
            "_source" : ["absolutePath", "data"],
            "query" : {{ "wildcard": {{ "absolutePath": {{
                "value": "*",
                "boost": 1.0,
                "rewrite": "constant_score" }} }} }} }}' """).format(**locals()))
    if rc != 0: out = None
    return out

def create_fulltext_index(index_name = DEFAULT_FULLTEXT_INDEX, port = ELASTICSEARCH_PORT):
    OPTION = "?include_type_name" if es7_or_later()  else "" #--> ES7 allows 'text' mapping but requires hint
    lib.execute_command("curl -X PUT -H'Content-Type: application/json' http://localhost:{port}/{index_name}".format(**locals()))
    lib.execute_command("curl -X PUT -H'Content-Type: application/json' http://localhost:{port}/{index_name}/_mapping/text{OPTION} ".format(**locals()) +
                        """ -d '{ "properties" : { "absolutePath" : { "type" : "keyword" }, "data" : { "type" : "text" } } }'""")
    return index_name

def create_metadata_index(index_name = DEFAULT_METADATA_INDEX, port = ELASTICSEARCH_PORT):
    OPTION = "" if es7_or_later() else "/text"               #--> switch away from 'text' mapping if using >= ES7
    lib.execute_command("curl -X PUT -H'Content-Type: application/json' http://localhost:{port}/{index_name}".format(**locals()))
    lib.execute_command("curl -X PUT -H'Content-Type: application/json' http://localhost:{port}/{index_name}/_mapping{OPTION} ".format(**locals()) +
                        """ -d '{ "properties" : {
                                "url": {
                                        "type": "text"
                                },
                                "zoneName": {
                                        "type": "keyword"
                                },
                                "absolutePath": {
                                        "type": "keyword"
                                },
                                "fileName": {
                                        "type": "text"
                                },
                                "parentPath": {
                                        "type": "text"
                                },
                                "isFile": {
                                        "type": "boolean"
                                },
                                "dataSize": {
                                        "type": "long"
                                },
                                "mimeType": {
                                        "type": "keyword"
                                },
                                "lastModifiedDate": {
                                        "type": "date",
                                        "format": "epoch_millis"
                                },
                                "metadataEntries": {
                                        "type": "nested",
                                        "properties": {
                                                "attribute": {
                                                        "type": "keyword"
                                                },
                                                "value": {
                                                        "type": "text"
                                                },
                                                "unit": {
                                                        "type": "keyword"
                                                }
                                        }
                                }
                            }}' """)
    return index_name

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
            admin_session.assert_icommand("iadmin rum") # remove unused metadata

    @staticmethod
    def fail_unless_delay_queue_is_empty(session_):
        session_.assert_icommand("iqstat","STDOUT_SINGLELINE","No delayed rules")

    def remove_all_jobs_from_delay_queue(self, session_, initial_sleep=0.0):
        if initial_sleep > 0.0:
            sleep( initial_sleep )
        session_.assert_icommand('iqdel -a', 'STDERR')
        self.fail_unless_delay_queue_is_empty( session_ )

    def delay_queue_is_empty(self, session_, soft_assert = False, try_kill = False, verbose = False):
        if try_kill:
            out,_,rc = session_.run_icommand('iqdel -a')
            if isinstance(try_kill, (float,int,long_type)) and try_kill > 0.0: sleep(try_kill)
        out,_,rc = session_.run_icommand('iqstat')
        if verbose:
            tempfile.NamedTemporaryFile(prefix='delay-queue-debug--',mode='a',delete=False).write('***\n OUT = ['+out+']***\n')
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

                    self.assertIsNotNone ( repeat_until (operator.eq, True, transform = lambda x: 'recursive_rm_object_by_path' in x[0], debugPrinter = debugPrt )
                                             (lambda : admin_session.run_icommand('iqstat')) () )

                    debugPrt ("<<<< wait for object purge job()s to disappear")
                    self.assertIsNotNone ( repeat_until (operator.eq, False, transform = lambda x: 'recursive_rm_object_by_path' in x[0], debugPrinter = debugPrt )
                                             (lambda : admin_session.run_icommand('iqstat')) () )
                finally:
                    admin_session.assert_icommand('irm -fr {collection}'.format(**locals()))
                    delete_fulltext_index()


    @staticmethod
    def digits_at_front_of_string(x):
        y = re.match('(^\d+)',x)
        return "" if y is None else y.groups()[0]

    @staticmethod
    def active_task_ids(session,self_):
        rc,iqstat_output,_ = session.assert_icommand('iqstat', 'STDOUT')
        self_.assertEqual( rc, 0 )
        return set(filter(None,map(self_.digits_at_front_of_string, iqstat_output.split('\n'))))

    def test_graceful_handling_of_premature_deletion_62(self):
        test_path = ""
        indices=()
        jobs_before_delete = []
        jobs_after_delete = []
        try:
            indices = (
              create_metadata_index(DEFAULT_METADATA_INDEX),
              create_fulltext_index(DEFAULT_FULLTEXT_INDEX)
            )
            with indexing_plugin__installed(indexing_config={'minimum_delay_time':'20', 'maximum_delay_time':'23'}):
                with session.make_session_for_existing_admin() as admin_session:
                    test_session = admin_session
                    List_Active_Task_IDs = lambda : self.active_task_ids(test_session,self)
                    path_to_home = '/{0.zone_name}/home/{0.username}'.format(test_session)
                    test_path = path_to_home + "/test_issue_62"
                    data_1 = test_path + "/data1-post"
                    setup = [
                        # - create the items to be indexed
                        ('create',      ['-C',{'path':test_path, 'delete_tree_when_done': False }]),
                        ('add_AVU_ind', ['-C',{'path':test_path, 'index_name': DEFAULT_METADATA_INDEX, 'index_type':'metadata'}]),
                        ('add_AVU_ind', ['-C',{'path':test_path, 'index_name': DEFAULT_FULLTEXT_INDEX, 'index_type':'full_text'}]),
                        ('sleep_for',   ['',{'seconds':5}]),
                        ('wait_for',    [self.delay_queue_is_empty, {'num_iter':45,'interval':2.125,'threshold':2}]),
                        # - create a new data object be scheduled for indexing
                        ('create',      ['-d',{'path':data_1, 'content':'test__62 content'}]),
                        ('add_AVU',     ['-d',{'path':data_1,    'avu':('t-post-d1-attr','dataobj_meta','zz') }]),
                        ('call_function',[List_Active_Task_IDs,{'return_values':jobs_before_delete}]),
                        # - delete data object before AVU or content can be indexed
                        ('delete',      ['',{'path':data_1}]),
                        ('call_function',[List_Active_Task_IDs,{'return_values':jobs_after_delete}]),
                    ]
                    with self.logical_filesystem_for_indexing( setup, test_session ):
                        self.assertEqual( len(jobs_before_delete), len(indices))
                        purge_jobs = list(set(jobs_after_delete) - set(jobs_before_delete))
                        test_session.assert_icommand(['iqdel'] + purge_jobs)
                        self.assertEqual( len(List_Active_Task_IDs()), len(indices))
                        # - wait for indexing job(s) to complete
                        self.assertIsNotNone( repeat_until (operator.eq, True) (self.delay_queue_is_empty) (test_session) )
        finally:
            if test_path:
                with session.make_session_for_existing_admin() as admin_session:
                    admin_session.assert_icommand ('irm -fr {}'.format(test_path))
            if DEFAULT_METADATA_INDEX in indices: delete_metadata_index()
            if DEFAULT_FULLTEXT_INDEX in indices: delete_fulltext_index()

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


    def test_applying_indicators_ex_post_facto (self):
        with session.make_session_for_existing_admin() as admin_session:
            create_fulltext_index()
            create_metadata_index()
            test_session = admin_session
            path_to_home = '/{0.zone_name}/home/{0.username}'.format(test_session)
            test_path = path_to_home + "/test_indicators_ex_post_facto"
            data_1 = test_path + "/data1-post"
            coll_1 = test_path + "/coll-1-post"
            objects = [
                # -- create the items to be indexed
                ('create',      ['-C',{'path':test_path, 'delete_tree_when_done': True }]),
                ('create',      ['-C',{'path':coll_1}]),
                ('create',      ['-d',{'path':data_1, 'content':'caveat emptor'}]),
                ('add_AVU',     ['-d',{'path':data_1,    'avu':('t-post-d1-attr','dataobj_meta','zz') }]),
                ('add_AVU',     ['-C',{'path':coll_1,    'avu':('t-post-c1-attr','coll_meta','zz') }]),
                ('add_AVU',     ['-C',{'path':test_path, 'avu':('t-post-c0-attr','coll_meta0','zz0') }]),

                ('sleep_for',   ['',{'seconds':5}]),
                ('wait_for',    [self.delay_queue_is_empty, {'num_iter':45,'interval':2.125,'threshold':2}]),
                # apply indexing indicator AVUs ...
                ('add_AVU_ind', ['-C',{'path':test_path, 'index_name': DEFAULT_METADATA_INDEX, 'index_type':'metadata'}]),
                ('add_AVU_ind', ['-C',{'path':test_path, 'index_name': DEFAULT_FULLTEXT_INDEX, 'index_type':'full_text'}]), # -- test [#19]
                # ... and let the indexing happen
                ('sleep_for',   ['',{'seconds':5}]),
                ('wait_for',    [self.delay_queue_is_empty, {'num_iter':45,'interval':2.125,'threshold':2}]),
            ]
            hit_ = []
            func = make_number_of_hits_fcn (hit_)
            hit_source = lambda key:hit_[0]['hits']['hits'][0]['_source'][key]
            try:
                with indexing_plugin__installed():
                    with self.logical_filesystem_for_indexing (objects, test_session):
                        resultCount = func (search_index_for_object_path(DEFAULT_FULLTEXT_INDEX, 'data1-post''$',
                                                                         extra_source_fields = ',"data"'))
                        self.assertEqual (resultCount, 1)
                        self.assertTrue( hit_source('data') == 'caveat emptor' )

                        _ = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't-post-d1-attr'))
                        self.assertTrue( hit_source('absolutePath') == data_1 )

                        _ = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't-post-c1-attr')) # -- test [#42]
                        self.assertTrue( hit_source('absolutePath') == coll_1 )

                        _ = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't-post-c0-attr'))
                        self.assertTrue( hit_source('absolutePath') == test_path )

                    with self.logical_filesystem_for_indexing( # Wait for the full indexing purge (because of test_path recursive delete) to finish
                                                               [ ('sleep_for',   ['',{'seconds':5}]),
                                                                 ('wait_for',    [self.delay_queue_is_empty,
                                                                                  {'num_iter':45,'interval':2.125,'threshold':2}]) ]
                                                               ,test_session):
                                                               # ... then test that indexed data has gone away.
                        resultCount = func (search_index_for_object_path(DEFAULT_FULLTEXT_INDEX, 'data1-post''$'))
                        self.assertEqual (resultCount, 0)
                        resultCount = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't-post-d1-attr'))
                        self.assertEqual (resultCount, 0)
                        resultCount = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't-post-c1-attr'))
                        self.assertEqual (resultCount, 0)
                        resultCount = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't-post-c0-attr'))
                        self.assertEqual (resultCount, 0)
            finally:
                delete_fulltext_index()
                delete_metadata_index()

    def test_fulltext_and_metadata_indicators_on_same_colln__19(self):
        with session.make_session_for_existing_admin() as admin_session:
            create_fulltext_index()
            create_metadata_index()
            test_session = admin_session
            path_to_home = '/{0.zone_name}/home/{0.username}'.format(test_session)
            test_path = path_to_home + "/test_indexing_issue_19"
            data_1 = test_path + "/data1"
            coll_1 = test_path + "/coll-1"
            objects = [
                ('create',      ['-C',{'path':test_path, 'delete_tree_when_done': True }]),
                ('add_AVU_ind', ['-C',{'path':test_path, 'index_name': DEFAULT_METADATA_INDEX, 'index_type':'metadata'}]),
                ('add_AVU_ind', ['-C',{'path':test_path, 'index_name': DEFAULT_FULLTEXT_INDEX, 'index_type':'full_text'}]),

                ('sleep_for',   ['',{'seconds':5}]),
                ('wait_for',    [self.delay_queue_is_empty, {'num_iter':45,'interval':2.125,'threshold':2}]),

                ('create',      ['-C',{'path':coll_1}]),
                ('create',      ['-d',{'path':data_1, 'content':'carpe diem'}]),
                ('add_AVU',     ['-d',{'path':data_1, 'avu':('t19-d1-attr','dataobj_meta','zz') }]),
                ('add_AVU',     ['-C',{'path':coll_1, 'avu':('t19-c1-attr','coll_meta','zz') }]),
                ('add_AVU',     ['-C',{'path':test_path, 'avu':('t19-c0-attr','coll_meta0','zz0') }]),

                ('sleep_for',   ['',{'seconds':5}]),
                ('wait_for',    [self.delay_queue_is_empty, {'num_iter':45,'interval':2.125,'threshold':2}]),
            ]
            hit_ = []
            func = make_number_of_hits_fcn (hit_)
            hit_source = lambda key:hit_[0]['hits']['hits'][0]['_source'][key]
            try:
                with indexing_plugin__installed():
                    with self.logical_filesystem_for_indexing (objects, test_session):
                        resultCount = func (search_index_for_object_path(DEFAULT_FULLTEXT_INDEX, 'data1''$', extra_source_fields = ',"data"')) #"data" => get content
                        self.assertEqual (resultCount, 1)
                        self.assertTrue( hit_source('data') == 'carpe diem' )
# dwm - explicit obj path will be needed when searching index for AVU
                        _ = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't19-d1-attr'))
                        self.assertTrue( hit_source('absolutePath') == data_1)
                        _ = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't19-c1-attr'))
                        self.assertTrue( hit_source('absolutePath') == coll_1)
                        _ = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't19-c0-attr'))
                        self.assertTrue( hit_source('absolutePath') == test_path)

                        # -- nesting the teardown phase within the overall scope prevents premature deletion of the top-level collection
                        with self.logical_filesystem_for_indexing ([ ('delete',    ['',{'path':data_1}]),
                                                                     ('delete',    ['',{'path':coll_1}]),
                                                                     ('rm_AVU',    ['-C',{'path':test_path, 'avu':('t19-c0-attr','coll_meta0','zz0') }]),
                                                                     ('sleep_for', ['',{'seconds':5}]),
                                                                     ('wait_for',  [self.delay_queue_is_empty, {'num_iter':45,'interval':2.125, 'threshold':2}]),
                                                                   ], test_session):
                            resultCount = func (search_index_for_object_path(DEFAULT_FULLTEXT_INDEX, 'data1$')) # -- test [#37]
                            self.assertEqual (resultCount, 0)
                            resultCount = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't19-d1-attr')) # -- test [#46]
                            self.assertEqual (resultCount, 0)
                            resultCount = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't19-c1-attr')) # -- no issue # but need to test these
                            self.assertEqual (resultCount, 0)
                            resultCount = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX, 't19-c0-attr')) #    are also removed from index
                            self.assertEqual (resultCount, 0)
            finally:
                delete_fulltext_index()
                delete_metadata_index()
                #test_session.assert_icommand('irm -fr '+test_path, 'STDOUT', '')

    def test_indexing_of_odd_chars_and_json_in_metadata__41__43(self):
        with session.make_session_for_existing_admin() as admin_session:
            self.remove_all_jobs_from_delay_queue(admin_session)
            create_metadata_index()
            test_session = admin_session
            path_to_home = '/{0.zone_name}/home/{0.username}'.format(test_session)
            test_path = path_to_home + "/test"
            data_1 = test_path + "/data1"
            data_2 = test_path + "/data2"
            weird_String = ''.join(chr(c) for c in list(range(33,127)))+' \t\\'
            weird_Json = r'''{"0":["\""],"1":{"2":"\\"}}'''
            objects = [
                ('create',      ['-C',{'path':test_path, 'delete_tree_when_done': True }]),
                ('add_AVU_ind', ['-C',{'path':test_path, 'index_name': DEFAULT_METADATA_INDEX, 'index_type':'metadata'}]),
                ('create',      ['-d',{'path':data_1, 'content':'abc'}]),
                ('create',      ['-d',{'path':data_2, 'content':'def'}]),
                ('add_AVU',     ['-d',{'path':data_1, 'avu':('t-d1-attr',weird_String,'zz') }]),  # -- test [#41]
                ('add_AVU',     ['-d',{'path':data_2, 'avu':('t-d2-attr',weird_Json,'yy') }]),    # -- test [#43]
                ('sleep_for',   ['',{'seconds':5}]),
                ('wait_for',    [self.delay_queue_is_empty, {'num_iter':45,'interval':2.125,'threshold':2}]),
            ]
            try:
                with indexing_plugin__installed():
                    hit_ = []
                    func = make_number_of_hits_fcn (hit_)
                    with self.logical_filesystem_for_indexing (objects,test_session):
                        # -- find the AVU we set with the JSON string as a value (waiting version, but times out quickly)
                        self.assertIsNotNone( repeat_until (operator.eq, 1, transform = make_number_of_hits_fcn(hit_))
                                              (search_index_for_avu_attribute_name)
                                              (DEFAULT_METADATA_INDEX,'t-d2-attr') )
                        self.assertTrue( hit_ [0]['hits']['hits'][0]['_source']['metadataEntries'][0]['value'] == weird_Json )
                        # -- This test is similar to the previous test, but non-waiting and broken down to the elements:
                        resultCount = func (search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX,'t-d1-attr'))
                        self.assertEqual (resultCount, 1)
                        self.assertEqual( weird_String, hit_ [0]['hits']['hits'][0]['_source']['metadataEntries'][0]['value'] )
            finally:
                delete_fulltext_index()
                delete_metadata_index()

    @contextlib.contextmanager
    def logical_filesystem_for_indexing(self,objects,session):
        build_indicator_AVU = lambda index_name, index_type, technology = 'elasticsearch': [
            "irods::indexing::index",
            index_name+"::"+index_type,
            technology
        ]
        collections_to_delete = []
        try:
            for instruc,data in objects:
                argument_,details_ = data
                if instruc == 'create':
                    if argument_ == '-C':
                        session.assert_icommand('imkdir -p {0}'.format(details_['path']))
                        if details_.get('delete_tree_when_done'): collections_to_delete += [details_['path']]
                    elif argument_ == '-d':
                        with tempfile.NamedTemporaryFile() as t:
                            t.write(details_.get('content',''))
                            t.flush();
                            session.assert_icommand('iput -f {0} {1}'.format(t.name,details_['path']))
                elif instruc == 'delete':
                    session.assert_icommand(['irm', '-rf', details_['path']],'STDOUT','')
                elif instruc in ('add_AVU','set_AVU','rm_AVU'):
                    session.assert_icommand(['imeta',instruc.split('_')[0], argument_, details_['path']] + list(details_['avu']))
                elif instruc in ('set_AVU_ind','add_AVU_ind','rm_AVU_ind'):
                    cmd =  ['imeta',instruc.split('_')[0], '-C',
                           details_['path'] ] + build_indicator_AVU(details_['index_name'],details_['index_type'])
                    session.assert_icommand(cmd,'STDOUT','')
                elif instruc == 'wait_for':
                    self.assertIsNotNone(repeat_until (operator.eq, True, num_iter=details_['num_iter'], interval=details_['interval'], threshold=details_['threshold']) (argument_) (session))
                elif instruc == 'sleep_for':
                    sleep(details_['seconds']);
                elif instruc == 'call_function':
                    details_.get('return_values',[])[:] = argument_()
            yield
            #    --> control goes to "with" clause after setting up conditions from the 'objects' parameter
        finally:
            for p in collections_to_delete:
                session.assert_icommand(['irm', '-rf', p],'STDOUT','')

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
                                out,_,rc=admin_session.run_icommand("""iquest --no-page '%s/%s' "select COLL_NAME,DATA_NAME where COLL_NAME like"""\
                                                                    """ '%/{c}/books{i:03}%'" """.format(c=collection,i=n))
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

if __name__ == '__main__':
    unittest.main()
