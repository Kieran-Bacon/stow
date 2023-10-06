# Stow

Documentation can be found in [docs/index.md](docs/index.md)

Change log can be found here [docs/changelog.md](docs/changelog.md)

## Running docs

```
mkdocs serve
```

## Running tests

Run the tests with coverage and profiling on

```
pytest --cov-config=.coveragerc --cov=stow --cov-report html tests/
pytest --cov-config=.coveragerc --cov=stow --cov-report html --profile --profile-svg tests/
```
