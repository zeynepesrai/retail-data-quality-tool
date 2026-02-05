setup:
	python -m pip install -r requirements.txt
	python -m pip install -r requirements-dev.txt
	python -m pip install -e .

sample:
	python scripts/make_sample_data.py

run:
	python scripts/run_dq.py --input data/sample/online_retail_sample.csv --outdir outputs
