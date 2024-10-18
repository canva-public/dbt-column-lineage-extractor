from setuptools import setup, find_packages

setup(
    name='dbt_column_lineage_extractor',
    version='0.1.2b2',
    description='A package for extracting dbt column lineage',
    long_description=open('../README.md').read(),
    long_description_content_type='text/markdown',
    author='Wen Wu',
    author_email='wenwu@canva.com',
    url='https://github.com/canva-public/dbt-column-lineage-extractor',
    packages=find_packages(),
    install_requires=[
        'sqlglot[rs] == 25.24.5',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    entry_points={
        'console_scripts': [
            'dbt_column_lineage_direct=dbt_column_lineage_extractor.cli_direct:main',
            'dbt_column_lineage_recursive=dbt_column_lineage_extractor.cli_recursive:main',
        ],
    },
)
