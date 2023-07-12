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


# Running the new Indexer


```
  $ source new_indexer/.env.postgres
  $ cargo test
```

# Demo

## Creating collection

We will create a minimal collection using the following input data (curl/create_coll_minimal.json) (Note that these are dummy input, not identical to `Polybase` inputs):

  ```
  {
   "id":"pk/0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9/App001/App001Coll",
   "code":"@public collection App001Coll { id: string; constructor(id: string) { this.id = id; }}",
   "ast":
      {
         "kind":"collection",
         "namespace":{
            "kind":"namespace",
            "value":"pk/0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9/App001"
         },
         "name":"App002Coll",
         "attributes":[
            {
               "kind":"property",
               "name":"id",
               "type":{
                  "kind":"primitive",
                  "value":"string"
               },
               "directives":[

               ],
               "required":true
            },
            {
              "kind": "property",
              "name": "age",
              "type": {
                "kind": "primitive",
                "value": "number"
              },
              "directives": [],
              "required": true
            },
            {
              "kind": "property",
              "name": "salary",
              "type": {
                "kind": "primitive",
                "value": "number"
              },
              "directives": [],
              "required": false
            },
            {
               "kind":"method",
               "name":"constructor",
               "attributes":[
                  {
                     "kind":"parameter",
                     "name":"id",
                     "type":{
                        "kind":"primitive",
                        "value":"string"
                     },
                     "required":true
                  }
               ],
               "code":"this.id = id;"
            },
            {
               "kind":"directive",
               "name":"public",
               "arguments":[

               ]
            }
         ]
      }
}

  ```

  ```
~/dev/playground/db_redesign:master$ curl -X POST -H "Content-Type: application/json" -d @curl/create_coll_minimal.json localhost:9002/api/collections

{"collection":{"ast":{"attributes":[{"directives":[],"kind":"property","name":"id","required":true,"type":{"kind":"primitive","value":"string"}},{"directives":[],"kind":"property","name":"age","required":true,"type":{"kind":"primitive","value":"number"}},{"directives":[],"kind":"property","name":"salary","required":false,"type":{"kind":"primitive","value":"number"}},{"attributes":[{"kind":"parameter","name":"id","required":true,"type":{"kind":"primitive","value":"string"}}],"code":"this.id = id;","kind":"method","name":"constructor"},{"arguments":[],"kind":"directive","name":"public"}],"kind":"collection","name":"App002Coll","namespace":{"kind":"namespace","value":"pk/0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9/App001"}},"code":"@public collection App001C

  ```

Created row in `collections`:

```
id	code	ast	public_key	created_at	updated_at
------------------------------------------------
pk/0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9/App001/App001Coll	@public collection App001Coll { id: string; constructor(id: string) { this.id = id; }}	{"kind": "collection", "name": "App002Coll", "namespace": {"kind": "namespace", "value": "pk/0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9/App001"}, "attributes": [{"kind": "property", "name": "id", "type": {"kind": "primitive", "value": "string"}, "required": true, "directives": []}, {"kind": "property", "name": "age", "type": {"kind": "primitive", "value": "number"}, "required": true, "directives": []}, {"kind": "property", "name": "salary", "type": {"kind": "primitive", "value": "number"}, "required": false, "directives": []}, {"code": "this.id = id;", "kind": "method", "name": "constructor", "attributes": [{"kind": "parameter", "name": "id", "type": {"kind": "primitive", "value": "string"}, "required": true}]}, {"kind": "directive", "name": "public", "arguments": []}]}	NULL	2023-07-06 06:48:07.19121+00	2023-07-06 06:48:07.19121+00
```

The actual created collection table (hashed and encoded table name - "sb6xbwsr4s3zbx_7ujwduifzefc__app001coll"):

```
SELECT 
   table_name, 
   column_name, 
   data_type 
FROM 
   information_schema.columns
WHERE 
   table_name = 'sb6xbwsr4s3zbx_7ujwduifzefc__app001coll';


"table_name"	"column_name"	"data_type"
----------------------------------------
"sb6xbwsr4s3zbx_7ujwduifzefc__app001coll"	"data"	"jsonb"
"sb6xbwsr4s3zbx_7ujwduifzefc__app001coll"	"created_at"	"timestamp with time zone"
"sb6xbwsr4s3zbx_7ujwduifzefc__app001coll"	"updated_at"	"timestamp with time zone"
"sb6xbwsr4s3zbx_7ujwduifzefc__app001coll"	"id"	"text"

```


Indexes on the created collection table:

```
select * from pg_indexes where tablename='sb6xbwsr4s3zbx_7ujwduifzefc__app001coll';

"schemaname"	"tablename"	"indexname"	"tablespace"	"indexdef"
-------------------------------------------------------------
"public"	"sb6xbwsr4s3zbx_7ujwduifzefc__app001coll"	"sb6xbwsr4s3zbx_7ujwduifzefc__app001coll_pkey"		"CREATE UNIQUE INDEX sb6xbwsr4s3zbx_7ujwduifzefc__app001coll_pkey ON public.sb6xbwsr4s3zbx_7ujwduifzefc__app001coll USING btree (id)"
"public"	"sb6xbwsr4s3zbx_7ujwduifzefc__app001coll"	"idx_sb6xbwsr4s3zbx_7ujwduifzefc__app001coll_id"		"CREATE INDEX idx_sb6xbwsr4s3zbx_7ujwduifzefc__app001coll_id ON public.sb6xbwsr4s3zbx_7ujwduifzefc__app001coll USING btree (((data ->> 'id'::text)))"
"public"	"sb6xbwsr4s3zbx_7ujwduifzefc__app001coll"	"idx_sb6xbwsr4s3zbx_7ujwduifzefc__app001coll_age"		"CREATE INDEX idx_sb6xbwsr4s3zbx_7ujwduifzefc__app001coll_age ON public.sb6xbwsr4s3zbx_7ujwduifzefc__app001coll USING btree (((data ->> 'age'::text)))"
"public"	"sb6xbwsr4s3zbx_7ujwduifzefc__app001coll"	"idx_sb6xbwsr4s3zbx_7ujwduifzefc__app001coll_salary"		"CREATE INDEX idx_sb6xbwsr4s3zbx_7ujwduifzefc__app001coll_salary ON public.sb6xbwsr4s3zbx_7ujwduifzefc__app001coll USING btree (((data ->> 'salary'::text)))"

```


## Listing collections

```
~/dev/playground/db_redesign:master$ curl localhost:9002/api/collections | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  1135  100  1135    0     0  51807      0 --:--:-- --:--:-- --:--:-- 59736
{
  "collections": {
    "collections": [
      {
        "ast": {
          "attributes": [
            {
              "directives": [],
              "kind": "property",
              "name": "id",
              "required": true,
              "type": {
                "kind": "primitive",
                "value": "string"
              }
            },
            {
              "directives": [],
              "kind": "property",
              "name": "age",
              "required": true,
              "type": {
                "kind": "primitive",
                "value": "number"
              }
            },
            {
              "directives": [],
              "kind": "property",
              "name": "salary",
              "required": false,
              "type": {
                "kind": "primitive",
                "value": "number"
              }
            },
            {
              "attributes": [
                {
                  "kind": "parameter",
                  "name": "id",
                  "required": true,
                  "type": {
                    "kind": "primitive",
                    "value": "string"
                  }
                }
              ],
              "code": "this.id = id;",
              "kind": "method",
              "name": "constructor"
            },
            {
              "arguments": [],
              "kind": "directive",
              "name": "public"
            }
          ],
          "kind": "collection",
          "name": "App002Coll",
          "namespace": {
            "kind": "namespace",
            "value": "pk/0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9/App001"
          }
        },
        "code": "@public collection App001Coll { id: string; constructor(id: string) { this.id = id; }}",
        "id": "pk/0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9/App001/App001Coll",
        "public_key": null
      }
    ],
    "count": 1
  },
  "status": "OK"
}

```

## Getting a collection

```
~/dev/playground/db_redesign:master$ curl localhost:9002/api/collections/pk%2F0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9%2FApp001%2FApp001Coll | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  1106  100  1106    0     0  72600      0 --:--:-- --:--:-- --:--:-- 92166
{
  "collection": {
    "ast": {
      "attributes": [
        {
          "directives": [],
          "kind": "property",
          "name": "id",
          "required": true,
          "type": {
            "kind": "primitive",
            "value": "string"
          }
        },
        {
          "directives": [],
          "kind": "property",
          "name": "age",
          "required": true,
          "type": {
            "kind": "primitive",
            "value": "number"
          }
        },
        {
          "directives": [],
          "kind": "property",
          "name": "salary",
          "required": false,
          "type": {
            "kind": "primitive",
            "value": "number"
          }
        },
        {
          "attributes": [
            {
              "kind": "parameter",
              "name": "id",
              "required": true,
              "type": {
                "kind": "primitive",
                "value": "string"
              }
            }
          ],
          "code": "this.id = id;",
          "kind": "method",
          "name": "constructor"
        },
        {
          "arguments": [],
          "kind": "directive",
          "name": "public"
        }
      ],
      "kind": "collection",
      "name": "App002Coll",
      "namespace": {
        "kind": "namespace",
        "value": "pk/0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9/App001"
      }
    },
    "code": "@public collection App001Coll { id: string; constructor(id: string) { this.id = id; }}",
    "id": "pk/0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9/App001/App001Coll",
    "public_key": null
  },
  "status": "OK"
}
```


## Creating collection record

```
~/dev/playground/db_redesign:master$ curl -X POST -H "Content-Type: application/json" -d '{"id": "id1", "data": {"id": "id1", "name": "Bob", "age":42, "salary": 879.89}}' localhost:9002/api/collections/pk%2F0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9%2FApp001%2FApp001Coll
{"collection_record":{"data":{"age":42.0,"id":"id1","name":"Bob","salary":879.89},"id":"id1"},"status":"OK"}

~/dev/playground/db_redesign:master$ curl -X POST -H "Content-Type: application/json" -d '{"id": "id2", "data": {"id": "id2", "name": "Dave", "age":21, "salary": 345.89}}' localhost:9002/api/collections/pk%2F0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9%2FApp001%2FApp001Coll
{"collection_record":{"data":{"age":21.0,"id":"id2","name":"Dave","salary":345.89},"id":"id2"},"status":"OK"}


```

## Listing collection records

```
~/dev/playground/db_redesign:master$ curl localhost:9002/api/collections/pk%2F0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9%2FApp001%2FApp001Coll/records | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   185  100   185    0     0  15940      0 --:--:-- --:--:-- --:--:-- 20555
{
  "collection_records": [
    {
      "data": {
        "age": 21,
        "id": "id2",
        "name": "Dave",
        "salary": 345.89
      },
      "id": "id2"
    },
    {
      "data": {
        "age": 42,
        "id": "id1",
        "name": "Bob",
        "salary": 879.89
      },
      "id": "id1"
    }
  ],
  "status": "OK"
}

```

## Getting a collection record

```
~/dev/playground/db_redesign:master$ curl localhost:9002/api/collections/pk%2F0x1fdb4bead8bc2e85d4de75694b893de5dfb0fbe69e8ed1d2531c805db483ba350ea28d4b1c7acf6171d902586e439a04f22cb0827b08a29cbdf3dd0e5c994ec9%2FApp001%2FApp001Coll/records/id1 | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   108  100   108    0     0   6254      0 --:--:-- --:--:-- --:--:--  7714
{
  "collection_record": {
    "data": {
      "age": 42,
      "id": "id1",
      "name": "Bob",
      "salary": 879.89
    },
    "id": "id1"
  },
  "status": "OK"

```



