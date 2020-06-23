from setuptools import find_packages, setup

setup(
    name='tdmq',
    version='0.0.0',
    packages=find_packages(),
    package_data={
        'tdmq': [ 'alembic.ini',
                  'tdmq_db_migrations/*',
                  'tdmq_db_migrations/versions/*' ] },
    zip_safe=False,
    install_requires=[
        'flask==1.1.0',
        'click==7.0',
        'psycopg2-binary==2.8',
        'alembic==1.3.0',
        'prometheus_client==0.8.0',
        'gunicorn==20.0.4',
        'sphinx',
        'sphinxcontrib-httpdomain',
        'pytest'
    ],
)
