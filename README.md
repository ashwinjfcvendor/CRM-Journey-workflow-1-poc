

## Steps to Run the Temporal Workflow


```bash
python3 -m venv env


```bash
source env/bin/activate


```bash
docker compose up -d


wait for 1 mins for all the containers to come up 


```bash
python3 test_worker2.py


check localhost:8080 to se the status of workflow