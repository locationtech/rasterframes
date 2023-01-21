#!/usr/bin/env bash -e

# If `coverage` tool isn't installed: `{pip|conda} install coverage`

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "$( dirname "${BASH_SOURCE[0]}" )"/..

coverage run setup.py test && coverage html --omit='.eggs/*,setup.py' && open htmlcov/index.html
