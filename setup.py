from setuptools import setup, find_packages

try:
    with open('README.md', 'r') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ''

setup(name='scraperx',
      packages=find_packages(),
      version='0.0.3',
      license='MIT',
      description='ScraperX SDK',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Eddy Hintze',
      author_email="eddy@hintze.co",
      install_requires=['pyyaml',
                        'parsel',
                        'requests',
                        'boto3',
                        'deepdiff',
                        ],
      )
