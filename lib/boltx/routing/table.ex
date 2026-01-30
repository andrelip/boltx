defmodule Boltx.Routing.Table do
  @moduledoc """
  Represents a Neo4j cluster routing table.

  The routing table contains information about which servers can handle
  different types of operations:

  - **routers**: Servers that can provide routing information
  - **readers**: Servers that can handle read queries (followers)
  - **writers**: Servers that can handle write queries (leader)

  The table has a TTL (time-to-live) after which it should be refreshed.
  """

  @enforce_keys [:database]
  defstruct [
    :database,
    routers: [],
    readers: [],
    writers: [],
    ttl: 300,
    last_updated_at: nil,
    initialized_without_writers: false
  ]

  @type address :: String.t()

  @type t :: %__MODULE__{
    database: String.t() | nil,
    routers: [address()],
    readers: [address()],
    writers: [address()],
    ttl: non_neg_integer(),
    last_updated_at: integer() | nil,
    initialized_without_writers: boolean()
  }

  @doc """
  Creates a new routing table for a database.
  """
  @spec new(String.t() | nil) :: t()
  def new(database \\ nil) do
    %__MODULE__{
      database: database,
      last_updated_at: System.monotonic_time(:second)
    }
  end

  @doc """
  Parses routing information from a server response.

  The response format is:
  ```
  %{
    "servers" => [
      %{"addresses" => ["host:port"], "role" => "WRITE"},
      %{"addresses" => ["host:port", ...], "role" => "READ"},
      %{"addresses" => ["host:port", ...], "role" => "ROUTE"}
    ],
    "ttl" => 300
  }
  ```
  """
  @spec parse(map(), String.t() | nil) :: t()
  def parse(routing_info, database \\ nil) do
    servers = Map.get(routing_info, "servers", [])
    ttl = Map.get(routing_info, "ttl", 300)

    {routers, readers, writers} =
      Enum.reduce(servers, {[], [], []}, fn server, {routers, readers, writers} ->
        addresses = Map.get(server, "addresses", [])
        role = Map.get(server, "role", "")

        case role do
          "ROUTE" -> {routers ++ addresses, readers, writers}
          "READ" -> {routers, readers ++ addresses, writers}
          "WRITE" -> {routers, readers, writers ++ addresses}
          _ -> {routers, readers, writers}
        end
      end)

    %__MODULE__{
      database: database,
      routers: routers,
      readers: readers,
      writers: writers,
      ttl: ttl,
      last_updated_at: System.monotonic_time(:second),
      initialized_without_writers: writers == []
    }
  end

  @doc """
  Checks if the routing table is fresh for the given access mode.

  A table is considered fresh if:
  - It hasn't expired (current_time < last_updated_at + ttl)
  - It has routers
  - It has readers (for read access) or writers (for write access)
  """
  @spec is_fresh?(t(), :read | :write) :: boolean()
  def is_fresh?(%__MODULE__{last_updated_at: nil}, _access_mode), do: false

  def is_fresh?(%__MODULE__{} = table, access_mode) do
    not expired?(table) and
      has_routers?(table) and
      has_servers_for_access?(table, access_mode)
  end

  @doc """
  Checks if the routing table has expired based on TTL.
  """
  @spec expired?(t()) :: boolean()
  def expired?(%__MODULE__{last_updated_at: nil}), do: true

  def expired?(%__MODULE__{last_updated_at: last_updated, ttl: ttl}) do
    current_time = System.monotonic_time(:second)
    current_time >= last_updated + ttl
  end

  @doc """
  Checks if the routing table should be purged from memory.

  A table should be purged if it has been expired for more than 30 seconds.
  """
  @spec should_purge?(t()) :: boolean()
  def should_purge?(%__MODULE__{last_updated_at: nil}), do: true

  def should_purge?(%__MODULE__{last_updated_at: last_updated, ttl: ttl}) do
    purge_delay = 30
    current_time = System.monotonic_time(:second)
    current_time >= last_updated + ttl + purge_delay
  end

  @doc """
  Returns all server addresses in this routing table.
  """
  @spec all_servers(t()) :: [address()]
  def all_servers(%__MODULE__{routers: routers, readers: readers, writers: writers}) do
    (routers ++ readers ++ writers) |> Enum.uniq()
  end

  @doc """
  Updates the routing table with new information.
  """
  @spec update(t(), t()) :: t()
  def update(%__MODULE__{} = _old_table, %__MODULE__{} = new_table) do
    new_table
  end

  @doc """
  Removes an address from the writers list.
  Used when a NotALeader error is received.
  """
  @spec remove_writer(t(), address()) :: t()
  def remove_writer(%__MODULE__{writers: writers} = table, address) do
    %{table | writers: List.delete(writers, address)}
  end

  @doc """
  Removes an address from the readers list.
  """
  @spec remove_reader(t(), address()) :: t()
  def remove_reader(%__MODULE__{readers: readers} = table, address) do
    %{table | readers: List.delete(readers, address)}
  end

  @doc """
  Removes an address from all server lists.
  """
  @spec deactivate_server(t(), address()) :: t()
  def deactivate_server(%__MODULE__{} = table, address) do
    %{table |
      routers: List.delete(table.routers, address),
      readers: List.delete(table.readers, address),
      writers: List.delete(table.writers, address)
    }
  end

  # Private functions

  defp has_routers?(%__MODULE__{routers: routers}) do
    routers != []
  end

  defp has_servers_for_access?(%__MODULE__{readers: readers}, :read) do
    readers != []
  end

  defp has_servers_for_access?(%__MODULE__{writers: writers}, :write) do
    writers != []
  end
end
