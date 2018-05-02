all : wheel

source_distribution : clean
		python setup.py sdist

wheel : source_distribution
		python setup.py bdist_wheel --universal

upload : wheel
		twine upload dist/*

install_local : wheel
		pip install -e .

install_distant :
		pip install flexiopy

clean :
		rm -rf build/ dist/ flexiopy.egg-info
