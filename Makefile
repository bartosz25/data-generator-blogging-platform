.PHONY : setup_project
setup_project:
	python setup.py bdist_wheel --universal

.PHONE : clean_build
clean_build:
	python setup.py clean --all

build_image:
	docker build  -t waitingforcode/data-generator-blogging-platform:0.1-beta -f docker/Dockerfile .

run_image:
	winpty docker run -ti data_generator_blogging_platform:0.1-beta  bash

release_docker_image:
	docker push waitingforcode/data-generator-blogging-platform:0.1-beta

test_all:
	pytest data_generator/

lint_all:
	flake8 data_generator/

reformat_all:
	autopep8 --in-place -r data_generator

test_coverage:
	pytest --cov=data_generator data_generator/test
