# Setup

## Environment

### PostgreSQL backend

1. Copy the `.env.postgres.example` file into `.env.postgres` and fill in the values. For example:


```
  $ cat .env.postgres

  export DATABASE_BACKEND=postgres

  export POSTGRES_HOST=127.0.0.1
  export POSTGRES_PORT=9000
  export POSTGRES_USER=polybase_dev
  export POSTGRES_PASSWORD=polybase_dev_password
  export POSTGRES_DB=polybase_dev.db

  export DATABASE_URL=postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}?schema=public
  export MAX_CONNECTIONS=100 # aligning to the postgres default

  export PGADMIN_PORT=9001
  export PGADMIN_DEFAULT_EMAIL=admin@admin.com
  export PGADMIN_DEFAULT_PASSWORD=pgadmin_password

  export INDEXER_SERVER_HOST=[::1]
  export INDEXER_SERVER_PORT=9003

```

2. Copy the `docker/.env.example` file ino `docker/.env` and fill in the values (the values must match those in the `.env.postgres` file`):

```
  $ cat docker/.env

  POSTGRES_PORT=9000
  PGADMIN_PORT=9001

```


To start the Docker services (first time):

```
  $ docker-compose -f docker/docker-compose.yml up -d
```

PgAdmin can be accessed at: `localhost:<PGADMIN_PORT>`.


For subsequent runs (in `dev`):

```
  $ docker-compose -f docker/docker-compose.yml start

```

to start the postgres db and pgadmin, and to stop them:

```
  $ docker-compose -f docker/docker-compose.yml stop
```

3. Run the migrations

```
$ source .env.postgres
$ sqlx migrate run --source migrations/postgres/

```


## Testing the new indexer


```
  $ cd new_indexer/tests

  $ source new_indexer/.env.postgres

  $ docker-compose -f docker/docker-compose.yml up -d

  $ cargo test
```
