# pyspark-deltalake
Creating a file with faker data and write in deltatable using pyspark, doing upsert for ingest new records, update or delete.  

## Installation

Create a venv with python.
```bash
python3 -m venv venv
```

Install requirements.txt file, available in root path.

```bash
pip install requirements.txt
```

Run __main__.py

```bash
python3 __main__.py
```

## How it works?

    We've a file.csv in raw path, you can change, delete or add any information and execute the program. After that, we can see in our delta table that records merging using specific keys.  

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.