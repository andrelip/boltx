defmodule Boltx.Routing.Router do
  @moduledoc """
  Manages routing tables for Neo4j clusters.

  This GenServer is responsible for:
  - Fetching and caching routing tables from the cluster
  - Refreshing routing tables when they expire
  - Selecting appropriate servers for read/write operations
  - Handling server failures and removing them from routing tables
  """

  use GenServer
  require Logger

  alias Boltx.Routing.Table
  alias Boltx.Client

  @default_refresh_interval 30_000  # 30 seconds

  defstruct [
    :initial_address,
    :config,
    :routing_context,
    routing_tables: %{},
    connections: %{},
    refresh_timer: nil
  ]

  # Public API

  @doc """
  Starts the router.

  Options:
  - `:initial_address` - The initial server address to connect to (required)
  - `:config` - Client configuration options
  - `:routing_context` - Additional routing context (default: %{})
  - `:name` - GenServer name (optional)
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Gets the routing table for a database.
  Refreshes the table if it's stale.
  """
  @spec get_routing_table(GenServer.server(), String.t() | nil) ::
          {:ok, Table.t()} | {:error, term()}
  def get_routing_table(router, database \\ nil) do
    GenServer.call(router, {:get_routing_table, database}, 30_000)
  end

  @doc """
  Ensures the routing table for a database is fresh.
  Returns true if the table was refreshed, false if it was already fresh.
  """
  @spec ensure_routing_table_fresh(GenServer.server(), String.t() | nil, :read | :write) ::
          {:ok, boolean()} | {:error, term()}
  def ensure_routing_table_fresh(router, database \\ nil, access_mode \\ :write) do
    GenServer.call(router, {:ensure_fresh, database, access_mode}, 30_000)
  end

  @doc """
  Selects a server address for the given access mode.

  Returns a server address from:
  - Writers list for `:write` access mode
  - Readers list for `:read` access mode
  """
  @spec select_server(GenServer.server(), String.t() | nil, :read | :write) ::
          {:ok, String.t()} | {:error, term()}
  def select_server(router, database \\ nil, access_mode \\ :write) do
    GenServer.call(router, {:select_server, database, access_mode}, 30_000)
  end

  @doc """
  Notifies the router that a write operation failed on a server.
  Removes the server from the writers list.
  """
  @spec on_write_failure(GenServer.server(), String.t(), String.t() | nil) :: :ok
  def on_write_failure(router, address, database \\ nil) do
    GenServer.cast(router, {:write_failure, address, database})
  end

  @doc """
  Notifies the router that a server is unavailable.
  Removes the server from all lists.
  """
  @spec deactivate_server(GenServer.server(), String.t(), String.t() | nil) :: :ok
  def deactivate_server(router, address, database \\ nil) do
    GenServer.cast(router, {:deactivate_server, address, database})
  end

  @doc """
  Forces a refresh of the routing table for a database.
  """
  @spec refresh_routing_table(GenServer.server(), String.t() | nil) ::
          {:ok, Table.t()} | {:error, term()}
  def refresh_routing_table(router, database \\ nil) do
    GenServer.call(router, {:refresh, database}, 30_000)
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    initial_address = Keyword.fetch!(opts, :initial_address)
    config = Keyword.get(opts, :config, [])
    routing_context = Keyword.get(opts, :routing_context, %{})

    state = %__MODULE__{
      initial_address: initial_address,
      config: config,
      routing_context: routing_context,
      routing_tables: %{},
      connections: %{}
    }

    # Schedule periodic cleanup of old routing tables
    timer = Process.send_after(self(), :cleanup, @default_refresh_interval)

    {:ok, %{state | refresh_timer: timer}}
  end

  @impl true
  def handle_call({:get_routing_table, database}, _from, state) do
    case get_or_create_table(state, database) do
      {:ok, table, new_state} ->
        {:reply, {:ok, table}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:ensure_fresh, database, access_mode}, _from, state) do
    table = Map.get(state.routing_tables, database)

    if table && Table.is_fresh?(table, access_mode) do
      {:reply, {:ok, false}, state}
    else
      case refresh_table(state, database) do
        {:ok, _table, new_state} ->
          {:reply, {:ok, true}, new_state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  def handle_call({:select_server, database, access_mode}, _from, state) do
    case get_or_create_table(state, database) do
      {:ok, table, new_state} ->
        result = select_from_table(table, access_mode)
        {:reply, result, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:refresh, database}, _from, state) do
    case refresh_table(state, database) do
      {:ok, table, new_state} ->
        {:reply, {:ok, table}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_cast({:write_failure, address, database}, state) do
    Logger.debug("Routing: removing writer #{address} for database #{inspect(database)}")

    new_state =
      update_in(state.routing_tables[database], fn
        nil -> nil
        table -> Table.remove_writer(table, address)
      end)

    {:noreply, new_state}
  end

  def handle_cast({:deactivate_server, address, database}, state) do
    Logger.debug("Routing: deactivating server #{address} for database #{inspect(database)}")

    new_state =
      update_in(state.routing_tables[database], fn
        nil -> nil
        table -> Table.deactivate_server(table, address)
      end)

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    # Remove expired routing tables
    new_tables =
      state.routing_tables
      |> Enum.reject(fn {_db, table} -> Table.should_purge?(table) end)
      |> Map.new()

    # Schedule next cleanup
    timer = Process.send_after(self(), :cleanup, @default_refresh_interval)

    {:noreply, %{state | routing_tables: new_tables, refresh_timer: timer}}
  end

  # Private functions

  defp get_or_create_table(state, database) do
    table = Map.get(state.routing_tables, database)

    cond do
      is_nil(table) ->
        refresh_table(state, database)

      Table.expired?(table) ->
        refresh_table(state, database)

      true ->
        {:ok, table, state}
    end
  end

  defp refresh_table(state, database) do
    # Try to get routing info from routers, falling back to initial address
    routers =
      case Map.get(state.routing_tables, database) do
        nil -> [state.initial_address]
        table -> table.routers ++ [state.initial_address]
      end
      |> Enum.uniq()

    fetch_routing_table_from_servers(routers, state, database)
  end

  defp fetch_routing_table_from_servers([], _state, _database) do
    {:error, :no_available_routers}
  end

  defp fetch_routing_table_from_servers([address | rest], state, database) do
    case fetch_routing_table_from_server(address, state, database) do
      {:ok, table} ->
        new_tables = Map.put(state.routing_tables, database, table)
        {:ok, table, %{state | routing_tables: new_tables}}

      {:error, reason} ->
        Logger.debug("Routing: failed to fetch from #{address}: #{inspect(reason)}")
        fetch_routing_table_from_servers(rest, state, database)
    end
  end

  defp fetch_routing_table_from_server(address, state, database) do
    # Parse the address into host:port
    {host, port} = parse_address(address)

    # Build config for this specific server
    config =
      state.config
      |> Keyword.put(:hostname, host)
      |> Keyword.put(:port, port)

    # Connect, initialize (HELLO/LOGON), then fetch routing table
    with {:ok, client} <- Client.connect(config),
         {:ok, _} <- init_connection(client, config),
         {:ok, routing_info} <- Client.send_route(client, state.routing_context, [], database) do
      Client.disconnect(client)
      table = Table.parse(routing_info, database)
      {:ok, table}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  # Initialize the connection based on Bolt version (send HELLO/LOGON)
  defp init_connection(client, opts) do
    bolt_version = client.bolt_version

    cond do
      bolt_version >= 5.1 ->
        with {:ok, _} <- Client.send_hello(client, opts),
             {:ok, _} <- Client.send_logon(client, opts) do
          {:ok, :initialized}
        end

      bolt_version >= 3.0 ->
        Client.send_hello(client, opts)

      true ->
        Client.send_init(client, opts)
    end
  end

  defp parse_address(address) when is_binary(address) do
    case String.split(address, ":") do
      [host, port_str] ->
        {host, String.to_integer(port_str)}

      [host] ->
        {host, 7687}
    end
  end

  defp select_from_table(table, :write) do
    case table.writers do
      [] ->
        {:error, :no_writers_available}

      writers ->
        # Simple random selection for load balancing
        {:ok, Enum.random(writers)}
    end
  end

  defp select_from_table(table, :read) do
    case table.readers do
      [] ->
        # Fall back to writers if no readers available
        case table.writers do
          [] -> {:error, :no_readers_available}
          writers -> {:ok, Enum.random(writers)}
        end

      readers ->
        {:ok, Enum.random(readers)}
    end
  end
end
