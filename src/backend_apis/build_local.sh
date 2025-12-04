#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Clean up old files
rm -rf local_env
rm -f dataplexutils_metadata_wizard-*.tar.gz

# Create and activate virtual environment
python3 -m venv local_env
source local_env/bin/activate

# Build the package with new file structure
cd ../package
python3 setup.py sdist
cp dist/dataplexutils_metadata_wizard-0.0.2.tar.gz ../backend_apis/
cd ../backend_apis

# Install dependencies
pip3 install -r requirements.txt

# Start the server
uvicorn main:app --reload

# Cleanup
deactivate

