
help:
	@echo - make release

clean:
	rm -rf *.egg-info build dist

release:
	rm -rf dist
	python3 setup.py sdist bdist_wheel
	python3 setup.py bdist_wheel
	twine upload -r pypi dist/*
