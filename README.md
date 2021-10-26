# Larkspur

A simple scalable bloom filter implementation, inspired by previous related work.
The goal of this project is not to be the best possible Redis-backed bloom filter,
but to be a good-enough Redis-backed bloom filter.

If you want the best possible Redis-backed bloom filter, you should consider
using the Redisbloom module of RedisLabs' Redis Enterprise Software.

## Installation

This project uses [poetry](https://python-poetry.org) for packaging and dependency management. So first you'll want to install `poetry` if you haven't already.

```
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -
```

After installing you will need to add the `poetry` executable to your `$PATH` and restart your shell.
The install script above should print out the correct path to use.

Now you can use `poetry` to install dependencies.

```
poetry install
```

`poetry` installs dependencies to a virtualenv automatically. To run tests, benchmarks, or anything else
that depends on dependencies installed through `poetry`, prefix the command with `poetry run`. E.g.,

```
poetry run larkspur/benchmarks.py
```

Alternatively you can activate the virtualenv for larkspur by running `poetry shell`. E.g.,

```
poetry shell
./larkspur/benchmarks.py
```

And then you can exit the virtualenv with `CTRL-d`.

## Development

Add dependencies with `poetry add`. Learn more about this command in the `poetry` [docs](https://python-poetry.org/docs/master).

To run tests:

```
poetry run pytest
```

## Deploying

Larkspur is included in `anhinga` as a dependency installed by `pip`. I wanted avoid known annoyances with installing dependencies directly from github/bitbucket so I decided to make this a public repo packaged on PyPI. So to deploy changes, you first have to upload a new release to PyPI. Then anhinga will use the new version the next time you deploy anhinga, assuming the semver query is compatible with the version of the new release.
To release a new version, make sure to first update the package version in `pyproject.toml`.
You will also need to configure your pypi API token. You can get the API token from the `larkspur-pypi-api-token` in AWS SecretsManager. Add it to your poetry config with:

```
poetry config pypi-token.pypi <the API token>
```

Now you can build and publish a new release to pypi:

```
poetry build
poetry publish -r pypi
```

Poetry

## Inspiration

- https://github.com/jaybaird/python-bloomfilter
- https://github.com/benhuds/yarb
- http://repositorium.sdum.uminho.pt/bitstream/1822/6627/1/dbloom_cmb.pdf
- http://www.isthe.com/chongo/tech/comp/fnv/index.html
- https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf

## Name

It's called Larkspur because the name is shared by a species of bird as well as a flower (bloom).
This project was originally created to serve a need at Feathr (www.feathr.co) and we
try to name all our projects along an avian theme.

Credit to Otis Stamp for coming up with the name!
