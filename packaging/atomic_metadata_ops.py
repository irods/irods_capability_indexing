#!/usr/bin/env python

from __future__ import print_function
import re
import sys
import getopt
from os.path import abspath,join

opt,arg = getopt.getopt(sys.argv[1:],'ts:v:rc') # -r to remove avu (default is to add)
                                                # -c object is collection (default: data-obj)
                                                # -t : tab separates tokens in text input
                                                # -v name : setup from named virtual environment
                                                # -s name : source input from file <name>, stdin if '-'
optD = dict(opt)

activate_path = optD.get('-v')

if activate_path is not None:
    if '/bin/activate' not in activate_path:
        activate_path = join(activate_path, 'bin/activate_this.py')
    activate_path = abspath( activate_path )
    exec(open(activate_path).read(), {'__file__': activate_path})

if optD.get('-t') is not None:
    sep = re.compile(r"[\t]")
else:
    sep = re.compile(r"[\t ]")

from irods.test.helpers import make_session
from irods.meta import (AVUOperation, iRODSMeta)

class NullInput(Exception): pass

def get_operation( source_line = '' , source_list = (), idx = 0 ):
   if isinstance( source_list, list ):
       l = source_list[idx:idx+4]
       del source_list[idx:idx+4]
       return l if len(l)==4 else ['']*4
   elif source_line:
       source_line = source_line.strip()
       args = sep.split(source_line)[:4]
       return args
   else:
       raise NullInput

if __name__ == '__main__':

    OP = 'remove' if '-r' in optD else 'add'

    collection_ =  ('-c' in optD)

    path_,arg = arg[0],arg[1:]

    def die(message='',code=123): print(message,file=sys.stderr); exit(code)

    with make_session() as ses:
        d = ses.data_objects.get(path_) if not collection_ else ses.collections.get(path_)
        operations = []
        file_ = None
        if '-s' in optD:
            file_ = sys.stdin if optD['-s'] == '-' else open(optD['-s'],'r')
        try:
            while True:
                if file_ is not None:
                    line = next(file_)
                    OP,a,v,u = get_operation (source_line=line)
                else:
                    OP,a,v,u = get_operation (source_list=arg)
                if not OP: break
                if OP[-1:] in ('-','+'): OP=('remove','add')[OP[-1:] == '+']
                operations += [ AVUOperation(operation = OP.lower(), avu = iRODSMeta(a, v, u)) ]
        except StopIteration:
            pass
        except NullInput:
            die("Null input",1)

        if operations:
            d.metadata.apply_atomic_operations(*operations)
