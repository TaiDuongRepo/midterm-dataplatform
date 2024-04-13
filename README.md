# Midterm Data Platform

# Step 1: Add studentID to the `.env` file

# Step 2: Add stduentID to the `app.py` file and `pipeline.py` file

# Step 3: Run the following command to start the application

cd to airflow

```bash
cd airflow/
```

Run docker compose

```bash
docker compose up --build
```

Stop docker compose by shortcut `Ctrl + C`

Add `pipeline.py` and `data_iuh_new.json` to the `dags` folder

Restart docker compose

```bash
docker compose up
```

# Step 4: Access the Airflow UI

Open your browser and access the Airflow UI at `http://localhost:8080/` login with username: `airflow` password: `airflow` and turn on the `studentID` DAG

Open qdrant dashboard at `http://localhost:6333/dashboard/` and check your collection `studentID`

# Step 5: Request /serach API

Using notebook file to request API

```python
import requests

url = "http://localhost:99xy"

response = requests.post(f"{url}/search", json = {"query": "Your query here"})

print(response.json())

```

# Step 6: Get 10 points
