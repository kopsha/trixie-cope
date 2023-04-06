# trixie can cope with heavy loads

Yet another multi-threaded cloud buckets uploader tool


## ToDO:

- [x] dockerize with mounted chunks volume
- [x] refactor to detect protocol from url (use FTP client)
- [x] integrate S3 boto client
- [x] integrate Google Storage client
- [x] pick storage credentials from genesis config
- [x] add unit tests
- [ ] integrate a python linter


## Configuration

```bash
## Environment variables
WORKERS = 4
RETRY_LIMIT = 3
ERROR_LIMIT = 13
```


## S3 Uploader tests

### 2 workers:

```bash
Uploaded 566 chunks, in 1 tries, with 0 errors (0.0 %).
and it took 125.789 s instead of 251.189 s.
```

### 4 workers

```bash
Uploaded 566 chunks, in 1 tries, with 0 errors (0.0 %).
and it took 66.323 s instead of 261.235 s.
```


### 8 workers

```bash
Uploaded 566 chunks, in 1 tries, with 0 errors (0.0 %).
and it took 40.176 s instead of 312.793 s.
```

```bash
Uploaded 566 chunks, in 1 tries, with 0 errors (0.0 %).
and it took 36.888 s instead of 289.906 s.
```


### 16 workers

```bash
Uploaded 566 chunks, in 1 tries, with 0 errors (0.0 %).
and it took 25.790 s instead of 388.309 s.
```

```bash
Uploaded 566 chunks, in 1 tries, with 0 errors (0.0 %).
and it took 26.874 s instead of 401.407 s.
```


## GCS Uploader tests

_NB:_ Please set the `GOOGLE_APPLICATION_CREDENTIALS` to the service account api
credentials file before running the script.


### 2 workers

```bash
Uploaded 566 chunks, in 1 tries, with 0 errors (0.0 %).
and it took 197.467 s instead of 394.387 s.
```


### 4 workers

```bash
Uploaded 566 chunks, in 1 tries, with 0 errors (0.0 %).
and it took 100.134 s instead of 399.997 s.
```


### 8 workers

```bash
Uploaded 566 chunks, in 1 tries, with 0 errors (0.0 %).
and it took 52.126 s instead of 415.040 s.
```


### 16 workers

```bash
Uploaded 566 chunks, in 1 tries, with 0 errors (0.0 %).
and it took 35.854 s instead of 563.631 s.

