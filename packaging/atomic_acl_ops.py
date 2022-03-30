#!/usr/bin/env python3

from __future__ import print_function
import sys
import json
import getopt
from os.path import (join, abspath)

opt,args = getopt.getopt(sys.argv[1:],'v:')
optD = dict(opt)
activate_path = optD.get('-v')
if activate_path is not None:
    if '/bin/activate' not in activate_path:
        activate_path = join(activate_path, 'bin/activate_this.py')
    activate_path = abspath( activate_path )
    exec(open(activate_path).read(), {'__file__': activate_path})

from irods.message import iRODSMessage, JSON_Message
from irods.test.helpers import  (home_collection, make_session)

def call_json_api(sess, request_text, api_number):
    with sess.pool.get_connection() as conn:
        request_msg = iRODSMessage("RODS_API_REQ",  JSON_Message( request_text, conn.server_version ), int_info=api_number)
        conn.send( request_msg )
        response = conn.recv()
    response_msg = response.get_json_encoded_struct()
    if response_msg:
        print("in atomic apply ACL api, server responded with: %r"%response_msg, file = sys.stderr)


def usage(program = sys.argv[0], stream = sys.stderr):
    print('{program} logical_path [user1 access1] [user2 access2] ... '.format(**locals()), file = stream)
    exit(1)

if __name__ == '__main__':

    try:
        logical_path = args.pop(0)
    except IndexError:
        usage()

    request = {"logical_path": logical_path,
            "operations": [ ]
           }

    with make_session() as ses:
        while len(args) > 0:
            try:
                username,access = args[:2]
            except ValueError:
                usage()
            del args[:2]
            request["operations"].append({"entity_name": username,
                                          "acl": access             })
        call_json_api(ses, request, api_number = 20005) # ATOMIC_APPLY_ACL_OPERATIONS_APN

