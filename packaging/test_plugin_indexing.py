from __future__ import print_function
import os
import sys
import shutil
import contextlib
import tempfile
import json
import re
import platform
import os.path

import zipfile
import subprocess
from time import sleep
from textwrap import dedent
import unittest

from ..configuration import IrodsConfig
from ..controller import IrodsController
from .resource_suite import ResourceBase
from ..test.command import assert_command
from . import session
from .. import test
from .. import paths
from .. import lib

import operator

long_type = int  # Python 3.x and up
try:
    long_type = long # Python 2.x
except NameError: pass

environment_variable_to_bool = lambda var:(
        os.environ.get(var,"").lower() not in ("n", "no", "f", "false", "0", ""))


Manual_Tests_Enabled = environment_variable_to_bool("MANUALLY_TEST_INDEXING_PLUGIN")

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

    basename = list(filter(any,SOURCE_BOOKS_URL.split("/")))[-1:]
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

ES_VERSION = '7.x' # TODO We probably don't need this anymore.

@contextlib.contextmanager
def indexing_plugin__installed(indexing_config=(), server_env_options={}):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['delay_server_sleep_time_in_seconds'] = 5

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
            }
        ]
        irods_config.commit(irods_config.server_config, irods_config.server_config_path)
        irods_config = IrodsConfig(**server_env_options)
        IrodsController(irods_config).restart()
        try:
            yield
        finally:
            pass


class Create_Virtualenv_Exception (Exception):
    def __init__(self,script_return_code,*a,**kw):
        """Initialize exception by storing return_code from the relevant failed shell command."""
        super(Create_Virtualenv_Exception,self).__init__(*a,**kw)
        self.script_return_code = script_return_code

def install_python3_virtualenv_with_python_irodsclient(PATH='~/py3',preTestPRCInstall=True):
    PATH = os.path.expanduser(PATH)
    rc = 0
    if preTestPRCInstall:
        rc = -1
        dummy_out,_,rc = lib.execute_command_permissive( dedent("""\
                . '{PATH}'/bin/activate ; cd ${{HOME}} ;  python -c 'import irods.manager; import remote_pdb'
                """.format(**locals())),use_unsafe_shell=True)
    if 0 != rc:
        os_dist = platform.dist()
        if os_dist[0].lower() == 'ubuntu' and os_dist[1] < '18.04':
            PIP_VERSION_LIMIT = "<21.0"
        else:
            PIP_VERSION_LIMIT = ""
        out,_,rc = lib.execute_command_permissive( dedent("""\
                python3 -m pip install --user --upgrade pip'{PIP_VERSION_LIMIT}' && \\
                python3 -m pip install --user virtualenv && \\
                python3 -m virtualenv -p python3 '{PATH}' && \\
                . '{PATH}'/bin/activate && \\
                python -m pip install remote_pdb python-irodsclient""".format(**locals())),use_unsafe_shell=True)
        if rc != 0: raise Create_Virtualenv_Exception(script_return_code = rc)
    return PATH


# Assuming use for metadata style of index only

def search_index_for_avu_attribute_name(index_name, attr_name, port = ELASTICSEARCH_PORT):
    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' http://localhost:{port}/{index_name}/_search?track_total_hits=true&rest_total_hits_as_int=true -d '
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
    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' http://localhost:{port}/{index_name}/_search?track_total_hits=true&rest_total_hits_as_int=true -d '
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
    out,_,rc = lib.execute_command_permissive( dedent("""\
        curl -X GET -H'Content-Type: application/json' http://localhost:{port}/{index_name}/_search?track_total_hits=true&rest_total_hits_as_int=true -d '
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
    mapping = json.dumps({
        "mappings": {
            "properties": {
                "absolutePath": {"type": "keyword"},
                "data": {"type": "text"}
            }
        }
    })
    lib.execute_command("curl -X PUT -H'Content-Type: application/json' http://localhost:{port}/{index_name} -d'{mapping}'".format(**locals()))
    return index_name

def create_metadata_index(index_name = DEFAULT_METADATA_INDEX, port = ELASTICSEARCH_PORT):
    mapping = json.dumps({
        "mappings": {
            "properties": {
                "url": {"type": "text"},
                "zoneName": {"type": "keyword"},
                "absolutePath": {"type": "keyword"},
                "fileName": {"type": "text"},
                "parentPath": {"type": "text"},
                "isFile": {"type": "boolean"},
                "dataSize": {"type": "long"},
                "mimeType": {"type": "keyword"},
                "lastModifiedDate": {
                    "type": "date",
                    "format": "epoch_second"
                },
                "metadataEntries": {
                    "type": "nested",
                    "properties": {
                        "attribute": {"type": "keyword"},
                        "value": {"type": "text"},
                        "unit": {"type": "keyword"}
                    }
                }
            }
        }
    })
    lib.execute_command("curl -X PUT -H'Content-Type: application/json' http://localhost:{port}/{index_name} -d'{mapping}'".format(**locals()))
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
        if Manual_Tests_Enabled:
            cls.venv_dir = install_python3_virtualenv_with_python_irodsclient(PATH='~/py3')

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
            tempfile.NamedTemporaryFile(prefix='delay-queue-debug--',mode='a',delete=False).write('***\n OUT = [{}]***\n'.format(out).encode('utf-8'))
        condition = (rc == 0 and 'No delayed rules' in out)
        if soft_assert:
            self.assertTrue( condition )
        return condition

    def test_iput_with_metadata__92(self):
        try:
            create_metadata_index(DEFAULT_METADATA_INDEX)
            with indexing_plugin__installed():
                with session.make_session_for_existing_admin() as ses:
                    path_to_home = '/{0.zone_name}/home/{0.username}'.format(ses)
                    test_file = tempfile.NamedTemporaryFile()
                    test_path = path_to_home + "/test_issue_92"
                    setup = [ ('create',      ['-C',{'path':test_path, 'delete_tree_when_done': True }]),
                              ('add_AVU_ind', ['-C',{'path':test_path, 'index_name': DEFAULT_METADATA_INDEX, 'index_type':'metadata'}]),
                              ('sleep_for',   ['',{'seconds':5}]),
                              ('wait_for',    [self.delay_queue_is_empty, {'num_iter':45,'interval':2.125,'threshold':2}]) ]
                    with self.logical_filesystem_for_indexing( setup, ses ):
                        ses.assert_icommand(['iput', '--metadata', 'attr0;y;z;attr1;b;c', test_file.name, test_path], 'STDOUT')
                        self.assertIsNotNone( repeat_until (operator.eq, 1, transform = number_of_hits)
                                                                (search_index_for_avu_attribute_name)
                                                                (DEFAULT_METADATA_INDEX, 'attr0'))
                        self.assertEqual( 1, number_of_hits(search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX,'attr1')) )
        finally:
            delete_metadata_index(DEFAULT_METADATA_INDEX)
            with session.make_session_for_existing_admin() as ses:
                ses.assert_icommand("iadmin rum") # remove unused metadata

    def test_throttle_limit_45(self):
        def indexing_flag_exists(ses,coll):
            rc,out,_ = ses.assert_icommand(["iquest", "--no-page", "%s", "select COLL_NAME where META_COLL_ATTR_NAME = 'irods::indexing::flag' "
                                 " and COLL_NAME = '{coll}'".format(**locals())], 'STDOUT')
            return rc == 0 and out.strip() == coll
        def num_rules_in_queue(ses,coll):
            rc,out,_ = ses.assert_icommand(["iquest", "--no-page", "%s", """select count(RULE_EXEC_ID) where RULE_EXEC_NAME like '%"{coll}/%' """.format(**locals())], 'STDOUT')
            return -1 if rc != 0 else int(out.strip())
        logical_test_path = ''
        test_dir_path = 'test_45'
        try:
            create_metadata_index(DEFAULT_METADATA_INDEX)
            with session.make_session_for_existing_admin() as ses:
                NUM_DATA_OBJECTS = 30
                THROTTLE_LIMIT = (NUM_DATA_OBJECTS//2+1)
                # - Create collection of data objects (with attached AVUs) to be indexed.
                files = lib.make_large_local_tmp_dir(test_dir_path, NUM_DATA_OBJECTS, 10)

                HOME = '/{0.zone_name}/home/{0.username}'.format(ses,**locals())
                if indexing_flag_exists(ses, HOME):
                    ses.assert_icommand("""imeta rmw -C {HOME} irods::indexing::flag % %""".format(**locals())) # clear flag

                logical_test_path = '/{0.zone_name}/home/{0.username}/{test_dir_path}'.format(ses,**locals())
                ses.assert_icommand(['iput', '-r', test_dir_path, logical_test_path], 'STDOUT')

                for file_ in files:
                    ses.assert_icommand("imeta set -d {logical_test_path}/{0} attr_45 value_45 units_45".format(file_,**locals()), "STDOUT")
                with indexing_plugin__installed( indexing_config = {'minimum_delay_time':'15', 'maximum_delay_time':'25', 'collection_test_flag':HOME,
                                                                    "job_limit_per_collection_indexing_operation":str(THROTTLE_LIMIT)} ):
                    ses.assert_icommand("""imeta set -C {coll} """
                                        """irods::indexing::index {ftidx}::metadata elasticsearch""".format( coll = logical_test_path,
                                                                                                             ftidx = DEFAULT_METADATA_INDEX))
                    self.assertIsNotNone (repeat_until (operator.eq, True) (indexing_flag_exists) (ses,HOME))  # --> wait for flag set (collection operation has started)
                    sleep(9)

                    self.assertLessEqual (num_rules_in_queue(ses,logical_test_path), THROTTLE_LIMIT)

                    self.assertIsNotNone (repeat_until (operator.eq, False) (indexing_flag_exists) (ses,HOME))  # --> wait for flag deletion (collection operation is done)
                    self.assertIsNotNone (repeat_until (operator.eq, 0) (num_rules_in_queue) (ses,logical_test_path))
                    sleep(4)
                    self.assertEqual( NUM_DATA_OBJECTS, number_of_hits(search_index_for_avu_attribute_name(DEFAULT_METADATA_INDEX,'attr_45')) )
        finally:
            with session.make_session_for_existing_admin() as ses:
                if logical_test_path:
                    ses.assert_icommand("irm -rf {}".format(logical_test_path))
                shutil.rmtree(test_dir_path, ignore_errors=True)
                delete_metadata_index(DEFAULT_METADATA_INDEX)


    def test_response_to_elasticsearch_404_fulltext__issue_34(self):
        with indexing_plugin__installed():
            sleep(5)
            collection =  'fulltext_indexed_for_404_test'
            with session.make_session_for_existing_admin() as admin_session:
                self.remove_all_jobs_from_delay_queue(admin_session)
                admin_session.assert_icommand('imkdir -p {0}'.format(collection))
                f = tempfile.NamedTemporaryFile()
                f.write("the quick brown fox".encode('utf-8'))
                f.flush()
                create_fulltext_index()
                try:
                    admin_session.assert_icommand(['imeta', 'set', '-C', collection,
                                                   'irods::indexing::index',
                                                   '{}::full_text'.format(DEFAULT_FULLTEXT_INDEX),
                                                   'elasticsearch'])
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
                    admin_session.assert_icommand('imeta set -d {data_object_path} attr0 val0'.format(**locals()))

                    if debugFileName: debugPrt = lambda *a,**k: print(*a,file=open(debugFileName,'a'),**k)
                    else:             debugPrt = lambda *a,**k: None

                    self.assertIsNotNone (
                        repeat_until (operator.eq, 1, transform = number_of_hits, debugPrinter = debugPrt)
                                          (search_index_for_avu_attribute_name)
                                          (DEFAULT_METADATA_INDEX, 'attr0')
                    )
                    delete_metadata_index()
                    sleep(5) ; debugPrt (">>>>")
                    admin_session.assert_icommand('imeta rm -d {data_object_path} attr0 val0'.format(**locals()))

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
                            t.write(details_.get('content','').encode('utf-8'))
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

    @unittest.skipIf(not Manual_Tests_Enabled, "Only manual testing is enabled for tests that depend on a Python3 virtual env")
    def test_indexing_with_atomic_metadata_ops_66(self):
        with indexing_plugin__installed():
            test_coll = 'testcoll_66'
            create_metadata_index (DEFAULT_METADATA_INDEX)
            try:
                with session.make_session_for_existing_admin() as admin_session:
                    self.remove_all_jobs_from_delay_queue(admin_session)
                    admin_session.assert_icommand('imkdir -p {0}'.format(test_coll))
                    admin_session.assert_icommand("""imeta set -C {0} irods::indexing::index {1}::metadata elasticsearch""".format(
                                                  test_coll,DEFAULT_METADATA_INDEX))
                    admin_session.assert_icommand("itouch {0}/testobj".format(test_coll))

                    if debugFileName: debugPrt = lambda *a,**k: print(*a,file=open(debugFileName,'a'),**k)
                    else:             debugPrt = lambda *a,**k: None

                    # Add metadata using atomic metadata API, wait until jobs disappear and check results
                    # Also note: changing cwd to $HOME first to avoid picking up ~/scripts/irods as a package dir.
                    admin_session.assert_icommand("""cd ${{HOME}} ;  python3 ~/scripts/irods/test/atomic_metadata_ops.py -v {self.venv_dir} """
                                                  """ '/{0.zone_name}/home/{0.username}'/{test_coll}/testobj ADD ab bc cd ADD xy yz '' """.format(
                                                  admin_session,**locals()),use_unsafe_shell=True)
                    self.assertIsNotNone ( repeat_until (operator.eq, True) (self.delay_queue_is_empty) (admin_session) )
                    self.assertIsNotNone (
                        repeat_until (operator.eq, 1, transform = number_of_hits, debugPrinter = debugPrt)
                                          (search_index_for_avu_attribute_name)
                                          (DEFAULT_METADATA_INDEX, 'ab')
                    )

                    # Remove metadata using atomic metadata API, wait until jobs disappear and check results
                    admin_session.assert_icommand("""python3 ~/scripts/irods/test/atomic_metadata_ops.py -v {self.venv_dir} """
                                                  """ '/{0.zone_name}/home/{0.username}'/{test_coll}/testobj REMOVE ab bc cd""".format(
                                                  admin_session,**locals()),use_unsafe_shell=True)
                    self.assertIsNotNone ( repeat_until (operator.eq, True) (self.delay_queue_is_empty) (admin_session) )
                    self.assertIsNotNone (
                        repeat_until (operator.eq, 0, transform = number_of_hits, debugPrinter = debugPrt)
                                          (search_index_for_avu_attribute_name)
                                          (DEFAULT_METADATA_INDEX, 'ab')
                    )
            finally:
                with session.make_session_for_existing_admin() as admin_session:
                    admin_session.assert_icommand('irm -fr {0}'.format(test_coll))
                delete_metadata_index (DEFAULT_METADATA_INDEX)


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
                                data_obj_name = list(filter(any,map(lambda s:s.strip(),out.split('\n'))))[0]
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

    def test_purge_when_AVU_indicator_removed(self):
        test_path_1 = ""
        try:
            create_metadata_index()
            create_fulltext_index()
            with indexing_plugin__installed():
                with session.make_session_for_existing_admin() as admin_session:
                    test_session = admin_session
                    path_to_home = '/{0.zone_name}/home/{0.username}'.format(test_session)
                    test_path_1 = path_to_home + "/test_purges_1"
                    data_1 = test_path_1 + "/data1-post"
                    QUOTE = ('The rocket stood in the cold winter morning, making summer with every breath of its mighty exhausts.'
                             '  - Ray Bradbury, "The Martian Chronicles".')
                    initial_setup = [
                        # - create the items to be indexed
                        ('create',      ['-C',{'path':test_path_1, 'delete_tree_when_done': False }]),
                        ('add_AVU_ind', ['-C',{'path':test_path_1, 'index_name': DEFAULT_METADATA_INDEX, 'index_type':'metadata'}]),
                        ('add_AVU_ind', ['-C',{'path':test_path_1, 'index_name': DEFAULT_FULLTEXT_INDEX, 'index_type':'full_text'}]),
                        ('create',      ['-d',{'path':data_1, 'content': QUOTE }]),
                        # - cause a new data object AVU to be indexed
                        ('add_AVU',     ['-d',{'path':data_1, 'avu':('purge_test','dataobj_meta','zz') }]),
                        ('sleep_for',   ['',{'seconds':5}]),
                        # - let indexing jobs complete
                        ('wait_for',    [self.delay_queue_is_empty, {'num_iter':45,'interval':2.125,'threshold':2}]),
                    ]
                    with self.logical_filesystem_for_indexing( initial_setup, test_session ):
                        # - for baseline, make sure content and metadata has been indexed
                        self.assertEqual(1, number_of_hits(search_index_for_All_object_paths (DEFAULT_FULLTEXT_INDEX)))
                        self.assertEqual(1, number_of_hits(search_index_for_avu_attribute_name (DEFAULT_METADATA_INDEX,'purge_test')))
                        Test_setup = [
                                ('rm_AVU_ind', ['-C',{'path':test_path_1, 'index_name': DEFAULT_METADATA_INDEX, 'index_type':'metadata'}]),
                                ('rm_AVU_ind', ['-C',{'path':test_path_1, 'index_name': DEFAULT_FULLTEXT_INDEX, 'index_type':'full_text'}]),
                                ('wait_for',   [self.delay_queue_is_empty, {'num_iter':45,'interval':2.125,'threshold':2}]),
                        ]
                        with self.logical_filesystem_for_indexing( Test_setup, test_session ): # dwm
                            # - test that the appropriate purges have occurred
                            self.assertEqual(0, number_of_hits(search_index_for_All_object_paths (DEFAULT_FULLTEXT_INDEX)))
                            self.assertEqual(0, number_of_hits(search_index_for_avu_attribute_name (DEFAULT_METADATA_INDEX,'purge_test')))
        finally:
            with session.make_session_for_existing_admin() as admin_session:
                if test_path_1:
                    admin_session.assert_icommand ('irm -fr {}'.format(test_path_1))
            delete_metadata_index()
            delete_fulltext_index()

    def test_load_parameters_as_int_or_string__91(self):

        env_filename = paths.default_client_environment_path()
        with open(env_filename) as f:
            cli_env = json.load(f)
            cli_env['irods_log_level'] = 7
        new_client_env = json.dumps(cli_env, sort_keys=True, indent=4, separators=(',', ': '))

        server_config_filename = paths.server_config_path()
        with open(server_config_filename) as f:
            svr_cfg = json.load(f)
            svr_cfg['log_level']['legacy'] = 'debug'
        new_server_config = json.dumps(svr_cfg, sort_keys=True, indent=4, separators=(',', ': '))

        with lib.file_backed_up(server_config_filename), lib.file_backed_up(env_filename):
            with open(server_config_filename, 'w') as f:
                f.write(new_server_config)
            with open(env_filename, 'w') as f_env:
                f_env.write(new_client_env)
            config_0 = { 'minimum_delay_time': 20, 'maximum_delay_time': 25, 'job_limit_per_collection_indexing_operation': 2001 }
            for test_config in [ config_0,                                   # try once with integers ...
                                 {k:str(v) for k,v in config_0.items()} ]:   # and once with strings
                with session.make_session_for_existing_admin() as ses:
                    with indexing_plugin__installed(indexing_config = test_config):
                        log_offset = lib.get_file_size_by_path(paths.server_log_path())
                        ses.assert_icommand ('ils' , 'STDOUT');
                        lib.delayAssert(lambda : all(
                            1 <= lib.count_occurrences_of_string_in_log( log_path = paths.server_log_path(),
                                                                         string = "value of {name}: {value}".format(**locals()),
                                                                         start_index = log_offset)
                            for name, value in test_config.items()
                        ))

if __name__ == '__main__':
    unittest.main()
