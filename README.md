# Blogging platform data generation

# TODO: complete me!

# Test
## PyCharm
To launch the tests on PyCharm, you need to enable pytest as the test runner for the project. You can see how to do this
on [jetbrains.com page](https://www.jetbrains.com/help/pycharm/pytest.html)

## Command line
To execute all tests from command line, you can use `make test_all` command. To check test coverage, you can execute
`make test_coverage`.

# Development
## virtualenv
Setup a virtualenv environment:
```
virtualenv -p python3 .venv/
```

Activate the installed environment:
```
source .venv/bin/activate
```

Install dependencies (venv activated):
``` 
pip3 install -r requirements.txt
```

Desactivate the virtualenv:
```
deactivate
```
## Code checks
Check code format:
```
make lint_all
```

Reformat code:
```
make reformat_all
```

## Pre-commit hook setup
The hook will execute the code formatting before the commit and the unit tests before the push. To install
it, please use [Pre-commit plugin](https://pre-commit.com/) and `pre-commit install` command.
