#! /bin/bash

set -e

COVERAGE_MIN=${COVERAGE_MIN:-90}

COVERAGE_FILE="coverage.out"

get_coverage () {
  go tool cover -func=${1} 2>/dev/null | awk '/total:/ {print $3}'
}

print_coverage () {
  printf "%s\t%s\n" "${1}" "${2}"
}

# Output total coverages
TOTAL_COVERAGE=$(get_coverage ${COVERAGE_FILE})

print_coverage "TOTAL Test Coverage:" "${TOTAL_COVERAGE}"

# Validate coverage
if (( ${TOTAL_COVERAGE%.*} < ${COVERAGE_MIN} )); then
  printf "\n%s\n" "ERROR: Total test coverage threshold of ${COVERAGE_MIN}% not met."
  exit 1
else
  printf "\n%s\n" "PASS: Total test coverage threshold of ${COVERAGE_MIN}% has been met."
fi
