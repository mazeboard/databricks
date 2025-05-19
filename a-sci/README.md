# arlq-sci

# generate notebooks
$for f in `find src -type f -name '*.py'`; do  dir=`dirname $f`; name=`basename $f`; mkdir -p notebooks/$dir; python src/convert-databricks-to-jupyter.py $f notebooks/$dir/${name%.*}.ipynb; done