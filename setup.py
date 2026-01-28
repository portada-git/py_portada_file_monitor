from setuptools import setup

setup(name='py_portada_file_monitor',
    version='0.0.4',
    description='....... for PortADa project',
    author='PortADa team',
    author_email='jcbportada@gmail.com',
    license='MIT',
    url="https://github.com/portada-git/py_portada_file_monitor.git",
    packages=['portada_file_monitor'],
    py_modules=['file_event_handler'],
    install_requires=[
        "watchdog",
        "dagster_graphql"
    ],
    python_requires='>=3.12',
    zip_safe=False)
