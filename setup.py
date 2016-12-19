from setuptools import setup, find_packages


setup(
    name='aioraft',
    version='0.1alpha',
    description='Asyncio Raft algorithm',
    long_description=open('README.rst').read(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)'
        'Topic :: Internet',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: MacOS',
        'Operating System :: POSIX',
        'Operating System :: Microsoft',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    keywords='asyncio raft',
    author='lisael',
    author_email='lisael@lisael.org',
    url='https://github.com/lisael/aioraft',
    license='AGPLv3',
    packages=find_packages('aioraft', exclude=['tests', 'examples']),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=False,
    scripts=[],
    install_requires=[
        'msgpack-python',
        'async-timeout',
    ],
)
