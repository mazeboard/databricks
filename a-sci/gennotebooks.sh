#!/bin/bash

notebooksdir="notebooks"

for f in `find src -type f -name '*.py'`; 
do  
    dir=`dirname $f`; name=`basename $f`; 
    mkdir -p $notebooksdir/$dir; 
    python convert-databricks-to-jupyter.py $f $notebooksdir/$dir/${name%.*}.ipynb; 
done

cp src/init.py $notebooksdir/src
cp -r src/libs $notebooksdir/src
