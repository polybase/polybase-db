-- Add up migration script here

create table if not exists collections (
  id text primary key,
  code text not null,
  ast jsonb not null,
  public_key jsonb,
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

create table if not exists pending_transactions (
  id bytea primary key, -- RPO hash
  data jsonb not null
);

