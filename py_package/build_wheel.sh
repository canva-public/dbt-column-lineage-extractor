# config virtualenv
python3 -m venv venv
source venv/bin/activate

# install pip build packages
pip install twine
pip install wheel
pip install setuptools

# build package
python setup.py sdist bdist_wheel
