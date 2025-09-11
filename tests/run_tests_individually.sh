#!/bin/bash

# Script to run each pytest file individually
# This helps isolate test failures and see which specific files are causing issues
# 
# Usage:
#   ./run_tests_individually.sh                    # Run all test files
#   ./run_tests_individually.sh ./test_config.py  # Run specific test file

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to run a single test file
run_single_test() {
    local test_file="$1"
    echo -e "${YELLOW}Running: $test_file${NC}"
    echo "----------------------------------------"
    
    if python -m pytest "$test_file" -v; then
        echo -e "${GREEN}✓ PASSED: $test_file${NC}\n"
        return 0
    else
        echo -e "${RED}✗ FAILED: $test_file${NC}\n"
        return 1
    fi
}

# If a specific test file is provided as argument, run only that
if [ $# -eq 1 ]; then
    if [ -f "$1" ]; then
        echo -e "${BLUE}Running single test file: $1${NC}\n"
        run_single_test "$1"
        exit $?
    else
        echo -e "${RED}Test file not found: $1${NC}"
        exit 1
    fi
fi

# Otherwise run all test files
echo -e "${BLUE}Running all pytest files individually...${NC}\n"

# Counters
TOTAL_FILES=0
PASSED_FILES=0
FAILED_FILES=0

# Find all test files
TEST_FILES=$(find ./ -name "test_*.py" -type f | sort)

if [ -z "$TEST_FILES" ]; then
    echo -e "${RED}No test files found in ./ directory${NC}"
    exit 1
fi

# Count total files
TOTAL_FILES=$(echo "$TEST_FILES" | wc -l)
echo -e "${BLUE}Found $TOTAL_FILES test files${NC}\n"

# Array to store failed files
FAILED_LIST=()

# Run each test file
for test_file in $TEST_FILES; do
    if run_single_test "$test_file"; then
        ((PASSED_FILES++))
    else
        FAILED_LIST+=("$test_file")
        ((FAILED_FILES++))
        # Continue with other tests even if one fails
        set +e  # Don't exit on error for individual tests
    fi
done

set -e  # Re-enable exit on error

# Summary
echo "========================================"
echo -e "${BLUE}SUMMARY${NC}"
echo "========================================"
echo "Total files: $TOTAL_FILES"
echo -e "Passed: ${GREEN}$PASSED_FILES${NC}"
echo -e "Failed: ${RED}$FAILED_FILES${NC}"

if [ $FAILED_FILES -gt 0 ]; then
    echo -e "\n${RED}Failed files:${NC}"
    for failed_file in "${FAILED_LIST[@]}"; do
        echo "  - $failed_file"
    done
    exit 1
else
    echo -e "\n${GREEN}All tests passed!${NC}"
fi
