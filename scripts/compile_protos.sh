#!/bin/bash
# Compile protobuf files for Go, Python, and TypeScript

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROTO_DIR="$PROJECT_ROOT/api/proto"
OUTPUT_GO="$PROJECT_ROOT/api/proto"
OUTPUT_PYTHON="$PROJECT_ROOT/sdk/python-client/scitrera_aether_client/proto"
OUTPUT_TS="$PROJECT_ROOT/sdk/typescript/src/proto"

echo "=== Compiling Protocol Buffers ==="
echo "Project root: $PROJECT_ROOT"
echo "Proto dir: $PROTO_DIR"
echo "Python output: $OUTPUT_PYTHON"
echo "TypeScript output: $OUTPUT_TS"

# python venv activation (if exists)
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    echo "Activating Python virtual environment..."
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Ensure output directories exist
mkdir -p "$OUTPUT_GO"
mkdir -p "$OUTPUT_PYTHON"
mkdir -p "$OUTPUT_TS"

# Install Go protobuf plugins if not present
if ! command -v protoc-gen-go &> /dev/null; then
    echo "Installing protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "Installing protoc-gen-go-grpc..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

echo ""
echo "=== Generating Go Code ==="
cd "$PROTO_DIR"

protoc \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    aether.proto

if [ $? -eq 0 ]; then
    echo "✓ Go code generated successfully"
else
    echo "✗ Go code generation failed"
    exit 1
fi

# Run go fmt on generated Go files
echo ""
echo "=== Formatting Go Code ==="
gofmt -w "$OUTPUT_GO"/*.go 2>/dev/null || true

echo ""
echo "=== Generating Python Code ==="
cd "$PROTO_DIR"

# Check if python3 and grpcio-tools are available
if ! command -v python3 &> /dev/null; then
    echo "⚠ Warning: python3 not found, skipping Python code generation"
    echo "  Install Python 3 to generate Python protobuf code"
else
    # Try to generate Python code
    # Note: Requires grpcio-tools package (pip install grpcio-tools)
    if python3 -m grpc_tools.protoc --version &> /dev/null; then
        python3 -m grpc_tools.protoc \
            -I. \
            --python_out="$OUTPUT_PYTHON" \
            --pyi_out="$OUTPUT_PYTHON" \
            --grpc_python_out="$OUTPUT_PYTHON" \
            aether.proto

        if [ $? -eq 0 ]; then
            echo "✓ Python code generated successfully"

            # Fix imports in generated grpc file to use relative imports
            if [ -f "$OUTPUT_PYTHON/aether_pb2_grpc.py" ]; then
                sed -i 's/^import aether_pb2 as aether__pb2$/from . import aether_pb2 as aether__pb2/' "$OUTPUT_PYTHON/aether_pb2_grpc.py"
                echo "✓ Fixed relative imports in gRPC file"
            fi
        else
            echo "✗ Python code generation failed"
            exit 1
        fi
    else
        echo "⚠ Warning: grpcio-tools not found, skipping Python code generation"
        echo "  Install with: pip install grpcio-tools"
    fi
fi

echo ""
echo "=== Generating TypeScript Code ==="
cd "$PROTO_DIR"

TS_SDK_DIR="$PROJECT_ROOT/sdk/typescript"
PROTO_LOADER_GEN="$TS_SDK_DIR/node_modules/.bin/proto-loader-gen-types"

# The TypeScript SDK uses @grpc/proto-loader for dynamic proto loading at runtime.
# proto-loader-gen-types generates TypeScript type definitions for type safety.
if [ -f "$PROTO_LOADER_GEN" ]; then
    "$PROTO_LOADER_GEN" \
        --longs=String \
        --enums=String \
        --defaults \
        --oneofs \
        --grpcLib=@grpc/grpc-js \
        --outDir="$OUTPUT_TS" \
        --includeComments \
        aether.proto

    if [ $? -eq 0 ]; then
        echo "✓ TypeScript types generated successfully (proto-loader-gen-types)"
    else
        echo "✗ TypeScript type generation failed"
        exit 1
    fi
else
    echo "⚠ Warning: proto-loader-gen-types not found, skipping TypeScript type generation"
    echo "  Install with: cd sdk/typescript && npm install"
fi

echo ""
echo "=== Summary ==="
echo "Generated Go files in $OUTPUT_GO:"
ls -la "$OUTPUT_GO"/*.go 2>/dev/null || echo "    (none found)"

echo ""
echo "Generated Python files in $OUTPUT_PYTHON:"
ls -la "$OUTPUT_PYTHON"/aether_pb2*.py* 2>/dev/null || echo "    (none found)"

echo ""
echo "Generated TypeScript files in $OUTPUT_TS:"
ls -la "$OUTPUT_TS"/*.ts 2>/dev/null || echo "    (none found)"

# Create __init__.py in proto directory if it doesn't exist
if [ -f "$OUTPUT_PYTHON/aether_pb2.py" ] && [ ! -f "$OUTPUT_PYTHON/__init__.py" ]; then
    touch "$OUTPUT_PYTHON/__init__.py"
    echo "✓ Created __init__.py in proto directory"
fi

echo ""
echo "✓ Protobuf compilation complete!"