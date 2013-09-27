#!/usr/bin/env python

# Author: Lisa Glendenning <lglenden@cs.washington.edu>
# Script front end to edu.uw.zookeeper.safari.Main

import sys, os, shlex

def main(argv, environ):
    SAFARI_PREFIX = environ.get(
                    'SAFARI_PREFIX',
                    os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    SAFARI_LIB = environ.get(
                    'SAFARI_LIB',
                    os.path.join(SAFARI_PREFIX, 'lib'))
    SAFARI_ETC = environ.get(
                    'SAFARI_ETC',
                    os.path.join(SAFARI_PREFIX, 'etc'))
    
    SAFARI_LOG_CONFIG = environ.get('SAFARI_LOG_CONFIG')
    if not SAFARI_LOG_CONFIG:
        for f in 'log4j2-test.xml', 'log4j2.xml':
            log_config_file = os.path.join(SAFARI_ETC, f)
            if os.path.isfile(log_config_file):
                SAFARI_LOG_CONFIG = \
                    "-Dlog4j.configurationFile=%s" % log_config_file
                break
    
    JAVA = os.path.join(environ['JAVA_HOME'], 'bin', 'java') \
            if 'JAVA_HOME' in environ else 'java'
    
    JAVA_CLASSPATH = os.path.join(SAFARI_LIB, "*")
    JAVA_MAIN = "edu.uw.zookeeper.safari.Main"
    JAVA_ARGS = ['-classpath', JAVA_CLASSPATH]
    if SAFARI_LOG_CONFIG:
        JAVA_ARGS.append(SAFARI_LOG_CONFIG)
    if 'JAVA_ARGS' in environ:
        JAVA_ARGS.extend(shlex.split(environ['JAVA_ARGS']))

    args = [JAVA] + JAVA_ARGS
    args.append(JAVA_MAIN)
    args.extend(argv[1:])
    os.execvp(JAVA, args)

if __name__ == "__main__":
    main(sys.argv, os.environ)
