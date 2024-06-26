#!/bin/bash

set -e

# Navigate to the root directory of the git repository
cd "$(git rev-parse --show-toplevel)"

# Parse the current version from the package.json file in the cache package
CURRENT_VERSION=$(jq -r '.version' packages/cache/package.json)

echo "Current version: $CURRENT_VERSION"

# New version is one minor.minor version higher than the current version
NEW_VERSION=$(echo "$CURRENT_VERSION" | awk -F. '{print $1 "." $2 "." ($3 + 1)}')

echo "New version: $NEW_VERSION"

# Update the version in the package.json file in the cache package
jq -r --arg version "$NEW_VERSION" '.version = $version' packages/cache/package.json > packages/cache/package.json.tmp
mv packages/cache/package.json.tmp packages/cache/package.json

rm -rf node_modules && npm install && npm run build && cd packages/cache && npm publish

# sleep for 5 seconds to allow the publish to complete
sleep 2

cd "$HOME/dev/bscache"

# Parse the current version of @actions/cache dependency from the package.json file
CURRENT_VERSION=$(jq -r '.dependencies."@actions/cache"' package.json)

echo "Current version: $CURRENT_VERSION"

# Update this to npm:@useblacksmith/cache@"$NEW_VERSION" in the package.json file
NEW_VERSION=npm:@useblacksmith/cache@"$NEW_VERSION"

jq -r --arg version "$NEW_VERSION" '.dependencies."@actions/cache" = $version' package.json > package.json.tmp
mv package.json.tmp package.json

rm -rf node_modules && npm install && npm run build && git add . && git commit -m 'debugging' && git push --force
