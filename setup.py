import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='larkspur',
    version='0.1.1',
    author='Thomas R Storey',
    author_email='thomas@feathr.co',
    description='a Redis-backed scalable bloom filter',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://bitbucket.org/feathr/larkspur',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
