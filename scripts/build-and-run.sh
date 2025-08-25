#!/bin/bash

# ETL Enterprise Application - Build and Run Script
# This script demonstrates how to build and run the enterprise ETL application

set -e

echo "=========================================="
echo "ETL Enterprise Application - Build & Run"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if .NET 8.0 is installed
check_dotnet() {
    print_status "Checking .NET 8.0 installation..."
    if ! command -v dotnet &> /dev/null; then
        print_error ".NET 8.0 is not installed. Please install it first."
        exit 1
    fi
    
    DOTNET_VERSION=$(dotnet --version)
    if [[ $DOTNET_VERSION != 8.* ]]; then
        print_error "This application requires .NET 8.0. Found version: $DOTNET_VERSION"
        exit 1
    fi
    
    print_success "Found .NET version: $DOTNET_VERSION"
}

# Restore NuGet packages
restore_packages() {
    print_status "Restoring NuGet packages..."
    dotnet restore ETL.Enterprise.sln
    print_success "Packages restored successfully"
}

# Build the solution
build_solution() {
    print_status "Building ETL Enterprise solution..."
    dotnet build ETL.Enterprise.sln --configuration Release --no-restore
    print_success "Solution built successfully"
}

# Run unit tests
run_tests() {
    print_status "Running unit tests..."
    if [ -d "tests/ETL.Enterprise.Tests.Unit" ]; then
        dotnet test tests/ETL.Enterprise.Tests.Unit/ETL.Enterprise.Tests.Unit.csproj --configuration Release --no-build
        print_success "Unit tests completed"
    else
        print_warning "Unit test project not found, skipping tests"
    fi
}

# Create logs directory
create_logs_directory() {
    print_status "Creating logs directory..."
    mkdir -p logs
    print_success "Logs directory created"
}

# Run the application
run_application() {
    print_status "Starting ETL Enterprise Console Application..."
    print_status "Press Ctrl+C to stop the application"
    echo ""
    
    cd src/ETL.Enterprise.Console
    dotnet run --configuration Release --no-build
}

# Main execution
main() {
    echo ""
    print_status "Starting build and run process..."
    echo ""
    
    # Check prerequisites
    check_dotnet
    
    # Create logs directory
    create_logs_directory
    
    # Build process
    restore_packages
    build_solution
    
    # Run tests (optional)
    if [ "$1" = "--skip-tests" ]; then
        print_warning "Skipping tests as requested"
    else
        run_tests
    fi
    
    echo ""
    print_success "Build completed successfully!"
    echo ""
    
    # Ask user if they want to run the application
    read -p "Do you want to run the ETL Enterprise application? (y/n): " -n 1 -r
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        run_application
    else
        print_status "Application not started. You can run it manually with:"
        echo "cd src/ETL.Enterprise.Console && dotnet run"
    fi
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        echo "ETL Enterprise Application - Build and Run Script"
        echo ""
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --skip-tests    Skip running unit tests"
        echo "  --help, -h      Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0              Build, test, and optionally run the application"
        echo "  $0 --skip-tests Build and optionally run the application (skip tests)"
        echo ""
        exit 0
        ;;
    --skip-tests)
        main --skip-tests
        ;;
    "")
        main
        ;;
    *)
        print_error "Unknown option: $1"
        echo "Use --help for usage information"
        exit 1
        ;;
esac
