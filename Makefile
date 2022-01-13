# ----------------------------------
#          INSTALL & TEST
# ----------------------------------
install_requirements:
	@pip install -r requirements.txt

check_code:
	@flake8 scripts/* taxi_compass/*.py

black:
	@black scripts/* taxi_compass/*.py

test:
	@coverage run -m pytest tests/*.py
	@coverage report -m --omit="${VIRTUAL_ENV}/lib/python*"

ftest:
	@Write me

clean:
	@rm -f */version.txt
	@rm -f .coverage
	@rm -fr */__pycache__ */*.pyc __pycache__
	@rm -fr build dist
	@rm -fr taxi_compass-*.dist-info
	@rm -fr taxi_compass.egg-info

install:
	@pip install . -U

all: clean install test black check_code

count_lines:
	@find ./ -name '*.py' -exec  wc -l {} \; | sort -n| awk \
        '{printf "%4s %s\n", $$1, $$2}{s+=$$0}END{print s}'
	@echo ''
	@find ./scripts -name '*-*' -exec  wc -l {} \; | sort -n| awk \
		        '{printf "%4s %s\n", $$1, $$2}{s+=$$0}END{print s}'
	@echo ''
	@find ./tests -name '*.py' -exec  wc -l {} \; | sort -n| awk \
        '{printf "%4s %s\n", $$1, $$2}{s+=$$0}END{print s}'
	@echo ''

# ----------------------------------
#      UPLOAD PACKAGE TO PYPI
# ----------------------------------
PYPI_USERNAME=<AUTHOR>
build:
	@python setup.py sdist bdist_wheel

pypi_test:
	@twine upload -r testpypi dist/* -u $(PYPI_USERNAME)

pypi:
	@twine upload dist/* -u $(PYPI_USERNAME)

# ----------------------------------
#      UPLOAD PACKAGE TO GCP
# ----------------------------------

# project id - replace with your GCP project id
PROJECT_ID=taxi-compass-lewagon

# bucket name - replace with your GCP bucket name
BUCKET_NAME=gcf-sources-588878948076-asia-southeast1

# choose your region from https://cloud.google.com/storage/docs/locations#available_locations
REGION=asia-southeast1

set_project:
    @gcloud config set project ${PROJECT_ID}

create_bucket:
    @gsutil mb -l ${REGION} -p ${PROJECT_ID} gs://${BUCKET_NAME}
