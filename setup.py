from setuptools import setup, find_packages

try:
    with open('README.md', 'r') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ""

setup(name='scraperx',
      packages=find_packages(),
      version='0.6.0',
      python_requires='>=3.6.0',
      license="MIT",
      description="ScraperX SDK",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="Eddy Hintze",
      author_email="eddy@hintze.co",
      url="https://github.com/xtream1101/scraperx",
      install_requires=['pyyaml',
                        'parsel',
                        'requests',
                        'boto3',
                        'deepdiff',
                        'smart_open>=1.8.4',
                        'charset_normalizer',
                        ],
      )
