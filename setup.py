import ez_setup
ez_setup.use_setuptools()
from setuptools import setup, find_packages, Extension
import os

def get_version():
    """
    Gets the version number. Pulls it from the source files rather than
    duplicating it.
    """
    fn = os.path.join(os.path.dirname(__file__), 'src', 'connection', '__init__.py')
    try:
        lines = open(fn, 'r').readlines()
    except IOError:
        raise RuntimeError("Could not determine version number"
                           "(%s not there)" % (fn))
    version = None
    for l in lines:
        # include the ' =' as __version__ might be a part of __all__
        if l.startswith('__version__ =', ):
            version = eval(l[13:])
            break
    if version is None:
        raise RuntimeError("Could not determine version number: "
                           "'__version__ =' string not found")
    return version

def emit_warning(warn):
    print "=-"*40
    print warn
    print "=-"*40


PACKAGES = find_packages('src')
EXT_MODULES = []
SCRIPTS = [] 
REQUIREMENTS = ["psycopg2",]
DATA_FILES = [
    ("connection/config", # folder 
     ["src/connection/config/default.ini", ] # which files to install there
     ),
]

setup(
    name = "connection",
    version = get_version(),
    packages = PACKAGES,
    package_dir = {"": "src"},
    author = "Martijn Meijers",
    author_email ="b.m.meijers@tudelft.nl",
    description = "Small helper class for setting up a database connection to PostgreSQL",
    url = "http://gdmc.nl/~martijn/",
    ext_modules = EXT_MODULES,
    data_files=DATA_FILES,
    zip_safe=False,
    scripts=SCRIPTS,
    install_requires=REQUIREMENTS,
)
