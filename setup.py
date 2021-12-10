from setuptools import find_packages, setup

setup(
    name='tdmq',
    version='0.0.0',
    packages=find_packages(),
    package_data={
        'tdmq': [ 'alembic.ini',
                  'templates/*',
                  'tdmq_db_migrations/*',
                  'tdmq_db_migrations/versions/*' ] },
    zip_safe=False,
)
