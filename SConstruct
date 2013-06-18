import os

SetOption('implicit_cache', 1)
VariantDir('build', 'src', duplicate=0)
env = Environment()
env.SConsignFile()

#env['CXX'] = 'clang'

# debug/release mode
if ARGUMENTS.get('mode', 'debug') == 'release':
    print "*** Release build ***"
    env['CCFLAGS'] = ['-O3', '-DNDEBUG']
    env['PROJ_MODE'] = 'release'
else:
    print "*** Debug build ***"
    env['CCFLAGS'] = ['-g', '-Wall', '-DDEBUG']
    env['PROJ_MODE'] = 'debug'
#    env['CCFLAGS'] = ['-g', '-Wall', '-DDEBUG', '-pg']
#    env['LINKFLAGS'] = ['-pg']

# parallel build
num_cpu = int(os.environ.get('NUM_CPU', 4))
SetOption('num_jobs', num_cpu)

# build directories
env['PROJECT_HOME'] = os.getcwd()
env['PROJECT_LIB_PATH'] = env['PROJECT_HOME'] + '/lib'
env['PROJECT_BIN_PATH'] = env['PROJECT_HOME'] + '/bin'
env['PROJECT_INC_PATH'] = env['PROJECT_HOME'] + '/src/include'

env.Append(CPPPATH = env['PROJECT_INC_PATH'])
env.Append(LIBPATH = env['PROJECT_LIB_PATH'])

Export('env')

SConscript('build/SConscript')
SConscript('config/SConscript')
