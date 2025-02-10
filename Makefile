PIP=venv/bin/pip
PYTHON=venv/bin/python

init:
	$(PIP) install -r requirements.lock

IMPORTERS = name title_akas title_principals title_episode title_crew title_ratings

import-%:
	PYTHONPATH=$(PWD) $(PYTHON) importer/$(subst -,_,$*)_importer.py

import-all:
	@echo "⏳ Starting the import process..."
	@start_time=$$(date +%s); \
	for importer in $(IMPORTERS); do \
		echo "🚀 Processing: importer/$${importer}_importer.py..."; \
		step_start=$$(date +%s); \
		PYTHONPATH=$(PWD) $(PYTHON) importer/$${importer}_importer.py; \
		step_end=$$(date +%s); \
		step_time=$$((step_end - step_start)); \
		step_min=$$((step_time / 60)); \
		step_sec=$$((step_time % 60)); \
		echo "✅ Finished: importer/$${importer}_importer.py (⏱  $${step_min}m $${step_sec}s)"; \
		echo ""; \
		echo ""; \
		echo "-----------------------------------"; \
		echo ""; \
		echo ""; \
	done; \
	end_time=$$(date +%s); \
	total_time=$$((end_time - start_time)); \
	total_min=$$((total_time / 60)); \
	total_sec=$$((total_time % 60)); \
	echo "🎉 All imports completed in ⏱  $${total_min}m $${total_sec}s!"
