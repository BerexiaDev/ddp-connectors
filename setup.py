from setuptools import setup, find_packages

setup(
    name='ddp_connectors_lib',
    version='0.10.0',
    packages=find_packages(),
    # Add dependencies here
    install_requires=[
        "pyodbc",
        "psycopg2",
        "sqlalchemy",
        "cx_oracle",
        "loguru",
    ],
    description='Deepkube Data Platform Global Library',
    author='Berexia DEV Team',
    author_email='berexiadev@berexia.com',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
