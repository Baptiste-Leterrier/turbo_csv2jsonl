# turbo_csv2jsonl

turbo_csv2jsonl is a fun and experimental project aimed at outperforming pandas in a very specific use case: converting large CSV files to JSONL format using only Python's internal libraries. This project is likely full of holes and is very niche, but it was created as a challenge to see how efficient we could make it.

# Features
- Converts large CSV files to JSONL format.
- Utilizes Python's internal libraries for processing.
- Parallel processing using multiprocessing for improved performance.
- Memory-mapped file I/O for faster data access.

# Requirements
Python 3


# Usage
- Clone the repository:

```
git clone https://github.com/Baptiste-Leterrier/turbo_csv2jsonl.git
cd turbocsvparser
```

- Run the script:

```
python turbocsv.py
```

## File Structure
- turbocsv.py: Main script for processing CSV files.
- input.csv: Example input CSV file.
- output.jsonl: Output JSONL file.

# How It Works
The script reads the input CSV file, splits it into chunks, and processes each chunk in parallel using the multiprocessing library. 
It uses memory-mapped file I/O for faster data access and constructs JSONL lines from the CSV data.
The result is a JSONL file wich map the CSV header as key and the values as values.

# Example

## CSV data

| id_of_user | id_of_car | percentage_usage |
|------------|----------|------------------|
| 1          | 1        | 0.5              |
| 1          | 2        | 0.3              |
| 1          | 3        | 0.2              |
| 2          | 1        | 0.4              |
| 2          | 2        | 0.4              |
| 2          | 3        | 0.2              |
| 3          | 1        | 0.3              |
| 3          | 2        | 0.4              |
| 3          | 3        | 0.3              |
| 4          | 1        | 0.2              |

## JSONL

```json
{"id_of_user":"1","id_of_car":"1","percentage_usage":"0.5"}
{"id_of_user":"1","id_of_car":"2","percentage_usage":"0.3"}
{"id_of_user":"1","id_of_car":"3","percentage_usage":"0.2"}
{"id_of_user":"2","id_of_car":"1","percentage_usage":"0.4"}
{"id_of_user":"2","id_of_car":"2","percentage_usage":"0.4"}
{"id_of_user":"2","id_of_car":"3","percentage_usage":"0.2"}
{"id_of_user":"3","id_of_car":"1","percentage_usage":"0.3"}
{"id_of_user":"3","id_of_car":"2","percentage_usage":"0.4"}
{"id_of_user":"3","id_of_car":"3","percentage_usage":"0.3"}
{"id_of_user":"4","id_of_car":"1","percentage_usage":"0.2"}
```

# FAQ

### ❓ What about types ?
Overated in json, but you can open a PR

### ❓ What are rules about PR ?
You can edit anything but:
- Only python internals
- No offloading of the processing to cloud, FPGA, ASIC (Quantum accepted)

### ❓ What are the perf ?
Tested on a Macbook M1 Pro with a CSV file with 14060836 lines and about 912MB

```
time python turbocsv.py 
python turbocsv.py  18.27s user 5.80s system 572% cpu 4.203 total
```

 
