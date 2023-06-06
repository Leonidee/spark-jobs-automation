#!/bin/zsh
#
# Run checks related to code quality.
#
# This script is intended for both the CI and to check locally that code standards are
# respected. We run doctests here (currently some files only), and we
# validate formatting error in docstrings.
#
# Usage:
#   $ ./ci/code_checks.sh               # run all checks
#   $ ./ci/code_checks.sh code          # checks on imported code
#   $ ./ci/code_checks.sh doctests      # run doctests
#   $ ./ci/code_checks.sh docstrings    # validate docstring errors
#   $ ./ci/code_checks.sh single-docs   # check single-page docs build warning-free
#   $ ./ci/code_checks.sh notebooks     # check execution of documentation notebooks

if [ -n "$VIRTUAL_ENV" ]; then
    :
else
    source path/to/venv/bin/activate
fi


TESTING_PATH=$1 # path to testing file
PYTEST_TAG="actual"

# pytest --disable-warnings --full-trace --tb=native $1


if [ $# -eq 0 ]; then
    echo "Usage: $0 [-a arg_a] [-b arg_b]"
    exit 1
fi

TESTING_PATH=""
PYTEST_TAG="default_value"

while getopts ":a:b:" opt; do
  case $opt in
    a) TESTING_PATH="$OPTARG"
    ;;
    b) PYTEST_TAG="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

if [ -n "$arg_b" ]; then
    # do something if arg_b is provided
    echo "Argument b: $arg_b"
elif [ -n "$arg_a" ]; then
    # do something if arg_a is provided but not arg_b
    echo "Argument a: $arg_a"
else
    # do something if neither arg_a nor arg_b is provided
    echo "Usage: $0 [-a arg_a] [-b arg_b]"
    exit 1
fi
