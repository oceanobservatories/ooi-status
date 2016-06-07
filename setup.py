from setuptools import setup, find_packages

setup(
    name='OOINet Status',
    version='0.0.3',
    url='https://github.com/oceanobservatories/ooi-status',
    long_description=__doc__,
    packages=find_packages(exclude=['test']),
    include_package_data=True,
    zip_safe=False,
    install_requires=['Flask>=0.10',
                      'gevent>=1.1',
                      'alembic>=0.8.5',
                      'APScheduler>=3.0',
                      'cassandra-driver>=3.1',
                      'Flask-Compress>=1.3',
                      'futures>=3.0',
                      'pandas>=0.18',
                      'psycogreen>=1.0',
                      'psycopg2>=2.6',
                      'gunicorn',
                      'toolz>0.7',
                      'click>=6.6',
                      'SQLAlchemy>=1.0.12',
                      'kombu',
                      'requests',
                      'matplotlib',
                      'seaborn']
)
