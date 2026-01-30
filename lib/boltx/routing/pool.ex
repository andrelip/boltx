defmodule Boltx.Routing.Pool do
  @moduledoc """
  A routing-aware connection pool for Neo4j clusters.

  This module provides:
  - Connection pooling per server in the cluster
  - Automatic routing of read queries to followers and write queries to the leader
  - Retry logic for NotALeader errors
  - Automatic refresh of routing tables

  ## Usage

      # Start a routing pool
      {:ok, pool} = Boltx.Routing.Pool.start_link(
        uri: "neo4j+s://cluster.example.com:7687",
        auth: [username: "neo4j", password: "password"]
      )

      # Execute a read query (routes to followers)
      {:ok, result} = Boltx.Routing.Pool.query(pool, "MATCH (n) RETURN n", %{}, mode: :read)

      # Execute a write query (routes to leader)
      {:ok, result} = Boltx.Routing.Pool.query(pool, "CREATE (n:Test) RETURN n", %{})
  """

  use GenServer
  require Logger

  alias Boltx.Routing.{Router, Table}
  alias Boltx.{Error, Response}

  @default_pool_size 10
  @default_max_retries 3
  @default_retry_delay 100

  defstruct [
    :router,
    :config,
    :database,
    :pools_table,
    pool_size: @default_pool_size,
    max_retries: @default_max_retries,
    retry_delay: @default_retry_delay
  ]

  # Public API

  @doc """
  Starts a routing pool.

  ## Options

  - `:uri` - The Neo4j cluster URI (required)
  - `:auth` - Authentication options `[username: ..., password: ...]` (required)
  - `:pool_size` - Number of connections per server (default: #{@default_pool_size})
  - `:database` - Default database name (default: nil for default database)
  - `:max_retries` - Maximum retry attempts for cluster errors (default: #{@default_max_retries})
  - `:retry_delay` - Delay between retries in ms (default: #{@default_retry_delay})
  - `:name` - GenServer name (optional)
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Executes a Cypher query on the cluster.

  ## Options

  - `:mode` - Access mode, `:read` or `:write` (default: `:write`)
  - `:database` - Target database (default: pool's default database)
  - `:bookmarks` - Transaction bookmarks for causal consistency
  - `:timeout` - Query timeout in milliseconds
  """
  @spec query(GenServer.server(), String.t(), map(), keyword()) ::
          {:ok, Response.t()} | {:error, Error.t() | term()}
  def query(pool, statement, params \\ %{}, opts \\ []) do
    GenServer.call(pool, {:query, statement, params, opts}, timeout_from_opts(opts))
  end

  @doc """
  Executes a Cypher query, raising on error.
  """
  @spec query!(GenServer.server(), String.t(), map(), keyword()) :: Response.t()
  def query!(pool, statement, params \\ %{}, opts \\ []) do
    case query(pool, statement, params, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @doc """
  Gets the current routing table for a database.
  """
  @spec routing_table(GenServer.server(), String.t() | nil) ::
          {:ok, Table.t()} | {:error, term()}
  def routing_table(pool, database \\ nil) do
    GenServer.call(pool, {:routing_table, database})
  end

  @doc """
  Stops the routing pool.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(pool) do
    GenServer.stop(pool)
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    uri = Keyword.fetch!(opts, :uri)
    auth = Keyword.fetch!(opts, :auth)

    # Parse the initial address from URI
    parsed_uri = URI.parse(uri)
    initial_address = "#{parsed_uri.host}:#{parsed_uri.port || 7687}"

    # Build base config for connections
    config = [
      uri: uri,
      auth: auth
    ]

    # Start the router
    router_opts = [
      initial_address: initial_address,
      config: config
    ]

    {:ok, router} = Router.start_link(router_opts)

    # Create ETS table for storing connection pools
    pools_table = :ets.new(:server_pools, [:set, :public])

    state = %__MODULE__{
      router: router,
      config: config,
      database: Keyword.get(opts, :database),
      pools_table: pools_table,
      pool_size: Keyword.get(opts, :pool_size, @default_pool_size),
      max_retries: Keyword.get(opts, :max_retries, @default_max_retries),
      retry_delay: Keyword.get(opts, :retry_delay, @default_retry_delay)
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:query, statement, params, opts}, _from, state) do
    mode = Keyword.get(opts, :mode, :write)
    database = Keyword.get(opts, :database, state.database)

    result = execute_with_routing(state, statement, params, opts, mode, database, 0)
    {:reply, result, state}
  end

  def handle_call({:routing_table, database}, _from, state) do
    result = Router.get_routing_table(state.router, database || state.database)
    {:reply, result, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Stop all server pools
    :ets.foldl(
      fn {_address, pool}, _acc ->
        try do
          GenServer.stop(pool, :normal, 5000)
        rescue
          _ -> :ok
        end
      end,
      nil,
      state.pools_table
    )

    :ets.delete(state.pools_table)
    :ok
  end

  # Private functions

  defp execute_with_routing(state, statement, params, opts, mode, database, retry_count)
       when retry_count < state.max_retries do
    # Ensure routing table is fresh
    case Router.ensure_routing_table_fresh(state.router, database, mode) do
      {:ok, _refreshed} ->
        # Select a server for this operation
        case Router.select_server(state.router, database, mode) do
          {:ok, address} ->
            execute_on_server(state, address, statement, params, opts, mode, database, retry_count)

          {:error, :no_writers_available} when mode == :write ->
            # Refresh and retry
            case Router.refresh_routing_table(state.router, database) do
              {:ok, _table} ->
                execute_with_routing(state, statement, params, opts, mode, database, retry_count + 1)

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp execute_with_routing(_state, _statement, _params, _opts, _mode, _database, _retry_count) do
    {:error, Error.wrap(__MODULE__, :max_retries_exceeded)}
  end

  defp execute_on_server(state, address, statement, params, opts, mode, database, retry_count) do
    # Get or create a connection pool for this server
    case get_or_create_pool(state, address) do
      {:ok, pool} ->
        # Build the query
        extra = build_extra_params(opts, mode, database)
        query = %Boltx.Query{statement: statement, extra: extra}

        # Execute the query
        case DBConnection.execute(pool, query, params, opts) do
          {:ok, _query, result} ->
            {:ok, result}

          {:error, %Error{} = error} ->
            handle_query_error(state, error, address, statement, params, opts, mode, database, retry_count)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        # Server connection failed, try another server
        Logger.debug("Routing: Failed to connect to #{address}: #{inspect(reason)}")
        Router.deactivate_server(state.router, address, database)
        execute_with_routing(state, statement, params, opts, mode, database, retry_count + 1)
    end
  end

  defp handle_query_error(state, error, address, statement, params, opts, mode, database, retry_count) do
    cond do
      Error.not_a_leader?(error) ->
        # Remove from writers and retry
        Logger.debug("Routing: NotALeader error from #{address}, retrying on different server")
        Router.on_write_failure(state.router, address, database)
        Process.sleep(state.retry_delay)
        execute_with_routing(state, statement, params, opts, mode, database, retry_count + 1)

      Error.retryable_cluster_error?(error) ->
        # Retry on different server
        Logger.debug("Routing: Retryable cluster error from #{address}, retrying")
        Router.deactivate_server(state.router, address, database)
        Process.sleep(state.retry_delay)
        execute_with_routing(state, statement, params, opts, mode, database, retry_count + 1)

      true ->
        # Non-retryable error
        {:error, error}
    end
  end

  defp get_or_create_pool(state, address) do
    case :ets.lookup(state.pools_table, address) do
      [{^address, pool}] ->
        # Verify the pool is still alive
        if Process.alive?(pool) do
          {:ok, pool}
        else
          :ets.delete(state.pools_table, address)
          create_and_store_pool(state, address)
        end

      [] ->
        create_and_store_pool(state, address)
    end
  end

  defp create_and_store_pool(state, address) do
    case create_pool(state, address) do
      {:ok, pool} ->
        :ets.insert(state.pools_table, {address, pool})
        {:ok, pool}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_pool(state, address) do
    {host, port} = parse_address(address)

    # Build config for this server
    # Extract scheme and SSL settings from original URI
    original_uri = Keyword.get(state.config, :uri)
    parsed_uri = URI.parse(original_uri)

    pool_config = [
      hostname: host,
      port: port,
      scheme: parsed_uri.scheme,
      auth: Keyword.get(state.config, :auth),
      pool_size: state.pool_size
    ]

    case Boltx.start_link(pool_config) do
      {:ok, pool} ->
        Logger.debug("Routing: Created connection pool for #{address}")
        {:ok, pool}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_address(address) when is_binary(address) do
    case String.split(address, ":") do
      [host, port_str] -> {host, String.to_integer(port_str)}
      [host] -> {host, 7687}
    end
  end

  defp build_extra_params(opts, mode, database) do
    mode_str = if mode == :read, do: "r", else: "w"

    %{
      mode: mode_str,
      db: database,
      bookmarks: Keyword.get(opts, :bookmarks, [])
    }
  end

  defp timeout_from_opts(opts) do
    Keyword.get(opts, :timeout, 30_000)
  end
end
