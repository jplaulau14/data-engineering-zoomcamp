# Data Engineering Zoomcamp - Week 1
## Docker and SQL

### Docker
- Docker allows you to put everything an application needs inside a container - sort of a box that contains everything: OS, system level libraries, python, etc.
- You run this box on a host machine. The container is completely isolated from the host machine env.
- In the container you can have Ubuntu 18.04, while your host is running on Windows.
- You can run multiple containers on one host and they won’t have any conflict.
- An image = set of instructions that were executed + state. All saved in “image”
*- Installing docker: https://docs.docker.com/get-docker/*

### Running Postgres with Docker
**Windows**:
```
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v //c:/{PATH}/{FOLDER_NAME}:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13
```

**MacOS**
```
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/{FOLDER_NAME}:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13
```

> If you see that `ny_taxi_postgres_data` is empty after running the container, try these:
    - Deleting the folder and running Docker again (Docker will re-create the folder)
    - Adjust the permissions of the folder by running `sudo chmod a+rwx ny_taxi_postgres_data`

#### CLI for Postgres
Installing `pgcli`
```pip install pgcli```
Use `pgcli` to connect to Postgres
```pgcli -h localhost -p 5432 -u root -d ny_taxi```

### pgAdmin
**Running pgAdmin**
```
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    dpage/pgadmin4
```

#### Running Postgres and pgAdmin together
Create a network
```docker network create pg-network```
Run Postgres
```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v //c:/{PATH}/{FOLDER_NAME}:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```
Run pgAdmin
```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4
```

### Data Ingestion
Running locally
```
URL="{Enter data url}"
python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```
Build the image
```docker build -t taxi_ingest:v001```
Run the script with Docker
```
URL="{Enter data url}"
docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```

### Docker compose
- Lets us run multiple containers and link them in a network
Run it
```docker-compose up```
Run in detached mode
```docker-compose up -d```
Shutting it down
```docker-compose down```
> Note: to make pgAdmin configuration persistent, create a folder `data_pgadmin`. Change its permission via
```sudo chown 5050:5050 data_pgadmin```
> and mount it to the `/var/lib/pgadmin` folder:
```
services:
  pgadmin:
    image: dpage/pgadmin4
    volumes:
      - ./data_pgadmin:/var/lib/pgadmin
    ...
```

### Dockerizing the Ingestion Script
Convert jupyter notebook to python script
```jupyter nbconvert --to=script 01_test_ingest_data.ipynb```
Rename the file to `ingest_data.py`
```mv 01_test_ingest_data.py ingest_data.py```
Ingest the data using the Python script
```
export URL="{Enter data url}"
python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```
Run the [Dockerfile](https://www.example.com)
```
docker build -t taxi_ingest:v001 .

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pg-database \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```