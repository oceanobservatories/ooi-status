from setuptools import setup, find_packages

setup(
    name='ooinet_status',
    version='1.2.0',
    url='https://github.com/oceanobservatories/ooi-status',
    long_description=__doc__,
    packages=find_packages(exclude=['test', 'alembic']),
    include_package_data=True,
    zip_safe=False,
    entry_points={
          'console_scripts': [
              'ooi_status_monitor=ooi_status.status_monitor:main',
          ],
      },
)
