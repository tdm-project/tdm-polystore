#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

requirements_file=$(readlink -f "${1}")
# Must validate the argument explicitly.  The grep in the process substitution
# will fail silently.
if [[ ! -r "${requirements_file}" ]]; then
  echo "${requirements_file} is not a readable file!" >&2
  exit 2
fi

# This is nothing special.  We install the packages that look like they're pinned
# and later give the rest of the package list.
# The reason is that conda bogs down terribly when presented the whole requirements
# file and takes forever to solve the requirements.
#
# A second option we may try is to install each requirements line individually,
# one at a time; but this also has drawbacks.
echo "Installing pinned requirements from ${requirements_file}" >&2
conda install -c conda-forge -v --yes --file <(grep '==' "${requirements_file}")
echo "Installing free requirements from ${requirements_file}" >&2
conda install -c conda-forge -v --yes --file <(grep -v '==' "${requirements_file}")
echo "Cleaning conda environment" >&2
conda clean --all -y
