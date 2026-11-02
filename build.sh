#!/bin/sh
set -eu

uv export --frozen --no-dev --no-editable --quiet -o requirements.txt
rm -rf packages_export
uv pip install \
    --no-deps \
    --no-installer-metadata \
    --no-compile-bytecode \
    --python-platform aarch64-manylinux2014 \
    --python 3.13 \
    --quiet \
    --target packages_export \
    --refresh \
    -r requirements.txt
rm -f package.zip
(cd packages_export && zip -rq ../package.zip .)
zip -rq package.zip src

echo "Built package.zip"
