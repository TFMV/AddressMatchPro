!#/bin/bash

export CONFIG_PATH=/Users/thomasmcgeehan/AddressMatchPro/AddressMatchPro/config.yaml
export SCRIPT_PATH=/Users/thomasmcgeehan/AddressMatchPro/AddressMatchPro/python-ml/generate_embeddings.py

python -m spacy download en_core_web_md

pip install mkdocs mkdocs-material ghp-import

go run /Users/thomasmcgeehan/AddressMatchPro/AddressMatchPro/cmd/addressmatchpro/main.go

