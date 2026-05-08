# dbt Profiles

Copy:

```text
dbt/profiles/profiles.yml.example
```

to:

```text
dbt/profiles/profiles.yml
```

Then fill Snowflake credentials through environment variables or .env.

The Docker Compose dbt service mounts this directory to:

```text
/root/.dbt
```
