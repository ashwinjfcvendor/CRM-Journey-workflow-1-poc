

## Steps to Run the Temporal Workflow


```bash
python3 -m venv env
```


```bash
source env/bin/activate
```

```bash
pip install -r requirements.txt 
```

```bash
docker compose up -d
```


wait for 1 mins for all the containers to come up 


```bash
python3 test_worker2.py
```


### Check localhost:8080 to see the status of workflow
