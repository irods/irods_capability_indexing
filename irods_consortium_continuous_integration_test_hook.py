from __future__ import print_function

import optparse
import os
import re
import shutil
import glob
import tempfile
import irods_python_ci_utilities

def Indexing_PackageName_Regex( package_ext, technology = 'elasticsearch' ):
    tech = re.escape(technology)
    ext = re.escape(package_ext)
    return re.compile(
        r'irods-rule-engine-plugin-(document-type|{tech}|indexing)[-_][0-9].*\.{ext}$'.format(**locals())
    )

def get_matching_packages(directory,ext):
    pattern = Indexing_PackageName_Regex(ext)
    return [ os.path.join(directory,y) for y in os.listdir(directory) if pattern.match(y) ]

def get_build_prerequisites_all():
    return['gcc', 'swig']

def get_build_prerequisites_apt():
    pre_reqs = ['uuid-dev', 'libssl-dev', 'libsasl2-2', 'libsasl2-dev', 'python-dev']
    pre_reqs += ['openjdk-8-jre','curl', 'python-pip']
    return get_build_prerequisites_all()+pre_reqs

def get_build_prerequisites_yum():
    return get_build_prerequisites_all()+['which', 'java-1.8.0-openjdk-devel', 'libuuid-devel', 'openssl-devel', 'cyrus-sasl-devel', 'python-devel',
                                          'python-pip']

def get_build_prerequisites_zypper():
    return get_build_prerequisites_all()+['which', 'java-1_8_0-openjdk-devel','curl', 'python-pip']

def get_build_prerequisites():
    dispatch_map = {
        'Ubuntu': get_build_prerequisites_apt,
        'Centos': get_build_prerequisites_yum,
        'Centos linux': get_build_prerequisites_yum,
        'Opensuse': get_build_prerequisites_zypper,
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()

Java_Home = None
class WrongJavaAsDefault (RuntimeError): pass

def install_build_prerequisites():
    global Java_Home
    irods_python_ci_utilities.install_os_packages(get_build_prerequisites())
    java_version_check = re.compile('openjdk version[^\d]*1\.8\.',re.MULTILINE)
    java_version_text = '\n'.join(irods_python_ci_utilities.subprocess_get_output(['/usr/bin/java','-version'])[1:3])
    java_real_bin = os.path.realpath('/usr/bin/java')
    Java_Home = os.path.sep.join((java_real_bin.split(os.path.sep))[:-2])
    if not java_version_check.search( java_version_text ):
        raise WrongJavaAsDefault


class IndexerNotImplemented (RuntimeError): pass
class WrongNumberOfGlobResults (RuntimeError): pass

def install_indexing_engine (indexing_engine):
    if 'elasticsearch' in indexing_engine.lower():
        tempdir = tempfile.mkdtemp()
        url = 'https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.4.2-linux-x86_64.tar.gz'
        irods_python_ci_utilities.subprocess_get_output(['wget', '-q', url])
        tar_names = [x for x in url.split('/') if '.tar' in x]
        irods_python_ci_utilities.subprocess_get_output(['tar', '-C', tempdir, '--no-same-owner', '-xzf', tar_names[-1]])
        irods_python_ci_utilities.subprocess_get_output(['sudo','useradd','elastic','-s/bin/bash'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'chown','-R','elastic',tempdir])
        executables = glob.glob(os.path.join(tempdir,'*','bin','elasticsearch'))
        if len(executables) != 1 : raise WrongNumberOfGlobResults
        irods_python_ci_utilities.subprocess_get_output(
            '''sudo su elastic -c "env JAVA_HOME='{0}' {1} --daemonize -E discovery.type=single-node -E http.port=9100"'''.format(
            Java_Home,executables[0]),shell=True)
    else:
        raise IndexerNotImplemented


def main():
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory')
    parser.add_option('--built_packages_root_directory')
    parser.add_option('--indexing_engine', default='elasticsearch', help='Index/Search Platform needed for plugin test')
    parser.add_option('--test', metavar='dotted name')
    parser.add_option('--skip-setup', action='store_false', dest='do_setup', default=True)
    options, _ = parser.parse_args()

    built_packages_root_directory = options.built_packages_root_directory
    package_suffix = irods_python_ci_utilities.get_package_suffix()
    os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(built_packages_root_directory)

    if options.do_setup:
        install_build_prerequisites()

        irods_python_ci_utilities.subprocess_get_output(['sudo', '-EH', 'pip', 'install', 'unittest-xml-reporting==1.14.0'])

        install_indexing_engine(options.indexing_engine)

        # Packages are put either in top level or os-specific subdirectory.
        # For indexing it seems to be top level for now. But just in case, we check both.
        for directory in ( built_packages_root_directory, os_specific_directory ):
            pkgs = get_matching_packages(directory, package_suffix)
            if pkgs:
                irods_python_ci_utilities.install_os_packages_from_files( pkgs )
                break

    test = options.test or 'test_plugin_indexing'

    test_output_file = 'log/test_output.log'

    try:
        irods_python_ci_utilities.subprocess_get_output( ['sudo', 'su', '-', 'irods', '-c',
            'python2 scripts/run_tests.py --xml_output --run_s={} 2>&1 | tee {}; exit $PIPESTATUS'.format(test, test_output_file)],
            check_rc=True)

    finally:
        output_root_directory = options.output_root_directory
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            test_output_file = os.path.join('/var/lib/irods', test_output_file)
            if os.path.exists(test_output_file):
                shutil.copy(test_output_file, output_root_directory)

if __name__ == '__main__':
    main()
