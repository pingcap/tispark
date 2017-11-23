from setuptools import setup
setup(
    name = 'pytispark',
    packages = ['pytispark'],
    version = '0.1.3',
    description = 'TiSpark support for python',
    author = 'PingCAP',
    author_email = 'novemser@gmail.com',
    url = 'https://github.com/pingcap/tispark',
    keywords = ['tispark', 'spark', 'tidb', 'olap'],
    license='Apache 2.0',
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    install_requires = ['pyspark', 'py4j']
)