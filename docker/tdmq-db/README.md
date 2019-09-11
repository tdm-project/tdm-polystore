
# Database schema management for the TDM query engine's database

This image provides the database management for tdmq.  The
image is programmed to:
  1. if the PostgreSQL cluster doesn't exist
    - create the cluster
    - create the timescaleDB database specified by the DBNAME variable
    - load the schema under `initdb.d`;
  2. run migrations, if the environment variable `POSTGRES_RUN_MIGRATIONS=true`;
  3. Start the database only if `START_DB=true`.
 
In Kubernetes, the recommended usage is to first launch the image as an init
container with `POSTGRES_RUN_MIGRATIONS=true` and without setting START_DB.
This way, you'll be able to configure the database before k8s will allow any
connections to it.  Then, you can re-run the image as the pod's main container
with `START_DB=true` to actually run the database server.


Except for this start-up phase, the image behaves just like the standard
timescaleDB image.   Database schema migrations are executed with alembic.

## Adding migrations

Schema migrations have to be added following [Alembic's
instructions](https://alembic.sqlalchemy.org/en/latest/tutorial.html#create-a-migration-script).

The image will only run migrations if the environment variable
`POSTGRES_RUN_MIGRATIONS` is set to `true`.

By default, the image will try to upgrate the DB to the `head` revision.  You
can specify an alternative target with the `POSTGRES_RUN_MIGRATIONS_TARGET`
environment variable.


## Adding to the DB initialization process

When a new DB is created, the [Postgresql Docker image documentation
applies](https://hub.docker.com/_/postgres#initialization-scripts). Any script
you add to the `initdb.d` directory in this repository will be copied into the
image by the Dockerfile.  Order of execution may be important so be careful with
the naming.


## Building

    docker build  -f Dockerfile.tdmq-db .

 
