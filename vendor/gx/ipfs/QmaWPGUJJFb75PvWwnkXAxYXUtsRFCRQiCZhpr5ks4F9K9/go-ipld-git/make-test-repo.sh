#!/usr/bin/env bash

CUR_DIR=$(pwd)
TEST_DIR=$(mktemp -d)
cd ${TEST_DIR}

git init

# Test generic commit/blob

git config user.name "John Doe"
git config user.email johndoe@example.com

echo "Hello world" > file
git add file

git commit -m "Init"

# Test generic commit/tree/blob, weird person info

mkdir dir
mkdir dir/subdir
mkdir dir2

echo "qwerty" > dir/f1
echo "123456" > dir/subdir/f2
echo "',.pyf" > dir2/f3

git add .

git config user.name "John Doe & John Other"
git config user.email "johndoe@example.com, johnother@example.com"
git commit -m "Commit 2"

# Test merge-tag
git config user.name "John Doe"
git config user.email johndoe@example.com

git branch dev
git checkout dev

echo ";qjkxb" > dir/f4

git add dir/f4
git commit -m "Release"
git tag -a v1 -m "Some version"
git checkout master

echo "mwvz" > dir/f5
git add dir/f5
git commit -m "Hotfix"

git merge v1 -m "Merge tag v1"

# Test encoding
git config i18n.commitencoding "ISO-8859-1"
echo "fgcrl" > f6
git add f6
git commit -m "Encoded"

# Test iplBlob/tree tags
git tag -a v1-file -m "Some file" 933b7583b7767b07ea4cf242c1be29162eb8bb85
git tag -a v1-tree -m "Some tree" 672ef117424f54b71e5e058d1184de6a07450d0e

# Create test archive, clean up

tar czf git.tar.gz .git
mv git.tar.gz ${CUR_DIR}/testdata.tar.gz
cd ${CUR_DIR}
#rm -rf ${TEST_DIR}
