# Migrating to Boltx Routing Pool

This document explains how to migrate from direct Boltx connections to the Routing Pool for proper Neo4j cluster support.

## Why This Change?

When using direct connections (`Boltx.start_link/1`), you connect to a single Neo4j server. In a cluster environment, this causes problems:

- **Write failures**: If connected to a follower node, write queries fail with `NotALeader` errors
- **No load balancing**: All queries hit a single server instead of distributing across the cluster
- **No failover**: If the connected server goes down, your application breaks

The Routing Pool solves these issues by:

- Discovering all cluster members automatically
- Routing write queries to the leader
- Load-balancing read queries across followers
- Retrying on `NotALeader` errors automatically
- Refreshing the routing table when leadership changes

## Migration Steps

### 1. Update Connection Setup

**Before:**
```elixir
# In your application.ex or supervisor
children = [
  {Boltx,
    name: :neo4j,
    uri: "neo4j+s://your-cluster.databases.neo4j.io",
    auth: [username: "neo4j", password: "your-password"]
  }
]
```

**After:**
```elixir
children = [
  {Boltx.Routing.Pool,
    name: :neo4j,
    uri: "neo4j+s://your-cluster.databases.neo4j.io",
    auth: [username: "neo4j", password: "your-password"],
    pool_size: 5  # connections per cluster member
  }
]
```

### 2. Update Query Calls

**Before:**
```elixir
Boltx.query(:neo4j, "MATCH (n:User) RETURN n")
Boltx.query(:neo4j, "CREATE (n:User {name: $name}) RETURN n", %{name: "Alice"})
```

**After:**
```elixir
# Read queries - routed to followers
Boltx.Routing.Pool.query(:neo4j, "MATCH (n:User) RETURN n", %{}, mode: :read)

# Write queries - routed to leader
Boltx.Routing.Pool.query(:neo4j, "CREATE (n:User {name: $name}) RETURN n", %{name: "Alice"}, mode: :write)
```

## Understanding the `mode` Parameter

The `mode` parameter tells the pool where to route your query:

| Mode | Routes To | Use For |
|------|-----------|---------|
| `:read` | Follower nodes | `MATCH`, `RETURN`, read-only queries |
| `:write` | Leader node | `CREATE`, `MERGE`, `DELETE`, `SET`, any data modifications |

### Examples

```elixir
# READ operations - use mode: :read
Boltx.Routing.Pool.query(:neo4j, "MATCH (n) RETURN count(n)", %{}, mode: :read)
Boltx.Routing.Pool.query(:neo4j, "MATCH (u:User {id: $id}) RETURN u", %{id: 123}, mode: :read)
Boltx.Routing.Pool.query(:neo4j, "CALL db.labels()", %{}, mode: :read)

# WRITE operations - use mode: :write
Boltx.Routing.Pool.query(:neo4j, "CREATE (n:User {name: $name})", %{name: "Bob"}, mode: :write)
Boltx.Routing.Pool.query(:neo4j, "MATCH (n) WHERE n.id = $id SET n.active = true", %{id: 1}, mode: :write)
Boltx.Routing.Pool.query(:neo4j, "MATCH (n:Temp) DELETE n", %{}, mode: :write)
Boltx.Routing.Pool.query(:neo4j, "MERGE (n:Config {key: $key}) SET n.value = $val", %{key: "k", val: "v"}, mode: :write)
```

### When in Doubt

If you're unsure which mode to use:

1. **Does the query modify data?** Use `:write`
2. **Is it read-only?** Use `:read`
3. **Still unsure?** Use `:write` (safe default, but less efficient)

## Helper Module (Optional)

To simplify the migration, you can create a wrapper module:

```elixir
defmodule MyApp.Neo4j do
  @pool :neo4j

  def read(query, params \\ %{}) do
    Boltx.Routing.Pool.query(@pool, query, params, mode: :read)
  end

  def write(query, params \\ %{}) do
    Boltx.Routing.Pool.query(@pool, query, params, mode: :write)
  end
end
```

Then use it like:
```elixir
MyApp.Neo4j.read("MATCH (n:User) RETURN n")
MyApp.Neo4j.write("CREATE (n:User {name: $name})", %{name: "Alice"})
```

## Configuration Options

```elixir
Boltx.Routing.Pool.start_link(
  name: :neo4j,                    # Process name (optional)
  uri: "neo4j+s://...",            # Cluster URI (required)
  auth: [username: "...", password: "..."],  # Auth (required)
  pool_size: 5,                    # Connections per server (default: 5)
  max_retry_count: 3,              # Max retries on failure (default: 3)
  retry_delay: 100,                # Delay between retries in ms (default: 100)
  routing_refresh_interval: 30_000 # Routing table refresh in ms (default: 30s)
)
```

## Compatibility

The Routing Pool works with all Neo4j deployments:

- Neo4j Aura (cloud)
- Neo4j Enterprise (clustered or single instance)
- Neo4j Community (single instance)

For single-instance deployments, it simply routes all queries to that instance.
