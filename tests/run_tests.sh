#!/bin/bash
# Run tests for Genie Audit Streamlit

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}╔════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║      Genie Audit - Test Runner             ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════╝${NC}"
echo ""

# Change to project directory
cd "$(dirname "$0")/.."

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo -e "${YELLOW}Installing test dependencies...${NC}"
    pip install pytest pytest-mock pytest-cov
fi

# Parse arguments
RUN_INTEGRATION=false
RUN_COVERAGE=false
VERBOSE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --integration|-i)
            RUN_INTEGRATION=true
            shift
            ;;
        --coverage|-c)
            RUN_COVERAGE=true
            shift
            ;;
        --verbose|-v)
            VERBOSE="-v"
            shift
            ;;
        --help|-h)
            echo "Usage: ./run_tests.sh [options]"
            echo ""
            echo "Options:"
            echo "  -i, --integration  Include integration tests (requires Databricks credentials)"
            echo "  -c, --coverage     Generate coverage report"
            echo "  -v, --verbose      Verbose output"
            echo "  -h, --help         Show this help"
            echo ""
            echo "Examples:"
            echo "  ./run_tests.sh                    # Run unit tests only"
            echo "  ./run_tests.sh --integration      # Run all tests including integration"
            echo "  ./run_tests.sh --coverage         # Run with coverage report"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Build pytest command
CMD="pytest $VERBOSE"

if [ "$RUN_INTEGRATION" = true ]; then
    echo -e "${BLUE}Running all tests (including integration)...${NC}"
else
    echo -e "${BLUE}Running unit tests only...${NC}"
    CMD="$CMD -m 'not integration'"
fi

if [ "$RUN_COVERAGE" = true ]; then
    CMD="$CMD --cov=. --cov-report=term-missing --cov-report=html"
fi

echo -e "Command: ${YELLOW}$CMD${NC}"
echo ""

# Run tests
eval $CMD

# Show coverage report location if generated
if [ "$RUN_COVERAGE" = true ]; then
    echo ""
    echo -e "${GREEN}Coverage report generated: htmlcov/index.html${NC}"
fi
