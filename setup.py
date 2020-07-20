from mypyc.build import mypycify
from setuptools import setup

setup(
    name='mavlink-influxdb',
    version='1.0',
    description="Upload MAVLink dataflash logs to InfluxDB",
    author="Ben Wolsieffer",
    author_email='benwolsieffer@gmail.com',
    license='MIT',

    ext_modules=mypycify(['mavlink_influxdb.py']),
    entry_points={
        'console_scripts': ['mavlink-influxdb=mavlink_influxdb:main'],
    },

    setup_requires=['mypy'],
    install_requires=['pymavlink', 'influxdb']
)
