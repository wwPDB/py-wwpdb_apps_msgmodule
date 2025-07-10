#!/bin/bash
# Quick Setup Script for wwPDB Communication Module Phase 2
# Handles installation issues automatically

set -e  # Exit on any error

echo "🚀 wwPDB Communication Module Phase 2 - Quick Setup"
echo "=================================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check Python version
print_status "Checking Python version..."
python3 --version

# Check if we're in the right directory
if [ ! -f "setup.py" ]; then
    print_error "setup.py not found. Please run this script from the project root directory."
    exit 1
fi

# Check for CMake
print_status "Checking for CMake..."
if command -v cmake >/dev/null 2>&1; then
    print_status "CMake found: $(cmake --version | head -n1)"
    HAS_CMAKE=true
else
    print_warning "CMake not found. Will install minimal dependencies."
    HAS_CMAKE=false
fi

# Install dependencies
print_status "Installing dependencies..."

if [ "$HAS_CMAKE" = true ]; then
    echo "Installing full dependencies..."
    pip3 install -r requirements.txt || {
        print_warning "Full installation failed. Trying minimal installation..."
        HAS_CMAKE=false
    }
fi

if [ "$HAS_CMAKE" = false ]; then
    echo "Installing minimal dependencies (skipping alignment tools)..."
    
    # Create minimal requirements file
    grep -v "wwpdb.utils.nmr" requirements.txt > requirements-minimal.txt || {
        print_error "Failed to create minimal requirements file"
        exit 1
    }
    
    pip3 install -r requirements-minimal.txt || {
        print_error "Failed to install minimal requirements"
        exit 1
    }
fi

# Install development tools
print_status "Installing development tools..."
pip3 install pytest pytest-cov pytest-mock pylint black flake8 safety bandit tox || {
    print_warning "Some development tools failed to install, continuing..."
}

# Install package in development mode
print_status "Installing package in development mode..."
pip3 install -e . || {
    print_error "Failed to install package in development mode"
    exit 1
}

# Create necessary directories
print_status "Creating necessary directories..."
mkdir -p logs tmp backups

# Test the installation
print_status "Testing installation..."
python3 -c "import wwpdb.apps.msgmodule; print('Package import successful')" || {
    print_error "Package import failed"
    exit 1
}

# Run Phase 2 validation
print_status "Running Phase 2 validation..."
python3 scripts/validate_phase2_integration.py || {
    print_warning "Phase 2 validation had some issues, but installation completed"
}

# Final status
print_status "Setup completed successfully!"
echo ""
echo "🎉 Quick Start Commands:"
echo "  make validate-phase2    # Validate Phase 2 implementation"
echo "  make test-phase2       # Run Phase 2 tests"
echo "  make feature-flags     # Show feature flag status"
echo "  make health           # Check system health"
echo "  make help             # Show all available commands"
echo ""

if [ "$HAS_CMAKE" = false ]; then
    print_warning "Note: CMake not found. Some optional features may not be available."
    echo "To install CMake:"
    echo "  macOS: brew install cmake"
    echo "  conda: conda install cmake"
    echo "  Then run: make install-dev"
fi

print_status "Ready for development!"
