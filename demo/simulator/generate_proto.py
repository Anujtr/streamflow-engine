#!/usr/bin/env python3
"""
Helper script to generate protobuf files using Docker
"""
import subprocess
import os
import sys

def generate_protobuf():
    """Generate protobuf files using Docker"""
    print("Generating protobuf files using Docker...")
    
    # Change to the simulator directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Create the Docker command to generate protobuf files
    cmd = [
        "docker", "run", "--rm", 
        "-v", f"{os.path.join(script_dir, '../..')}:/workspace",
        "-w", "/workspace",
        "namely/protoc-all:1.51_1",
        "-d", "demo/simulator",
        "-i", "api/proto",
        "-l", "python",
        "--with-grpc"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Protobuf files generated successfully!")
            return True
        else:
            print(f"❌ Error generating protobuf files: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Failed to run Docker command: {e}")
        return False

if __name__ == "__main__":
    if not generate_protobuf():
        print("Falling back to manual protobuf file creation...")
        sys.exit(1)