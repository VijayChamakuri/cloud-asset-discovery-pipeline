.PHONY: run sample test clean
run:    ; ./run.sh full
sample: ; ./run.sh sample
test:   ; PYSPARK_PYTHON=python3 pytest -q tests/
clean:  ; rm -rf data
