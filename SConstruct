import os

mymode = ARGUMENTS.get('mode', 'release')   #holds current mode

#check if the user has been naughty: only 'debug' or 'release' allowed
if not (mymode in ['debug', 'release']):
   print "Error: expected 'debug' or 'release', found: " + mymode
   Exit(1)

print '**** Compiling in ' + mymode + ' mode...'

debugcflags = [] #'-W1', '-GX', '-EHsc', '-D_DEBUG', '/MDd']   #extra compile flags for debug
releasecflags = [] # '-O2', '-EHsc', '-DNDEBUG', '/MD']         #extra compile flags for release

PROJECT_HOME   = os.getcwd()
SOURCE_HOME    = PROJECT_HOME + "/" + "src/"
BINARY_HOME    = PROJECT_HOME + "/" + "bin/" 
LIB_HOME       = PROJECT_HOME + "/" + "lib/" # /home/prakash/bigarchive/bigarchive_v1/lib"
INCLUDE_HOME   = SOURCE_HOME + "/" + "include/"
EXCEPTION_HOME = SOURCE_HOME + "/" + "exception/"

env = Environment()

print "PROJECT_HOME   : " + PROJECT_HOME
print "SOURCE_HOME    : " + SOURCE_HOME 
print "BINARY_HOME    : " + BINARY_HOME
print "LIB_HOME       : " + LIB_HOME
print "INCLUDE_HOME   : " + INCLUDE_HOME
print "EXCEPTION_HOME : " + EXCEPTION_HOME

# env.Append(LIBS = ['SDL_image','GL'])
#env.Append(LIBS = [
#external_libs + "/libqfs_client.so" ,
#external_libs + "/libqfs_common.so" ,
#external_libs + "/libqfs_emulator.so" ,
#external_libs + "/libqfs_io.so",
#external_libs + "/libqfs_meta.so",
#external_libs + "/libqfs_qcdio.so",
#external_libs + "/libqfs_qcrs.so",
#external_libs + "/libqfs_tools.so",
#])
#env.Append(LINKFLAGS = ['-Wl,--rpath,/usr/local/lib/'])


# make sure the sconscripts can get to the variables
Export('env', 'mymode', 'debugcflags', 'releasecflags', 'PROJECT_HOME', 'SOURCE_HOME', 'BINARY_HOME',
	'LIB_HOME', 'INCLUDE_HOME', 'EXCEPTION_HOME')

env.SConsignFile()

#specify the sconscript for file_system

project = 'exception'
SConscript(SOURCE_HOME + "/" + project + '/SConscript', exports=['project'])

project = 'fs'
SConscript(SOURCE_HOME + "/" + project + '/SConscript', exports=['project'])

project = 'append-store'
SConscript(SOURCE_HOME + "/" + project + "/SConscript", exports=['project'])

#project = 'test'
#SConscript(SOURCE_HOME + "/" + project + '/SConscript', exports=['project'])
