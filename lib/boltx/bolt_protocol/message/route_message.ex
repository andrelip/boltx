defmodule Boltx.BoltProtocol.Message.RouteMessage do
  @moduledoc """
  ROUTE message for Neo4j cluster routing.

  This message is used to retrieve routing information from the cluster.
  It is available in Bolt 4.3+ (signature 0x66).

  For older Bolt versions, we fall back to the `dbms.routing.getRoutingTable`
  stored procedure.

  Message format varies by Bolt version:
  - Bolt 4.3: ROUTE routing_context bookmarks database_name
  - Bolt 4.4+/5.x: ROUTE routing_context bookmarks {db: ..., imp_user: ...}
  """

  alias Boltx.BoltProtocol.MessageEncoder

  @signature 0x66

  @doc """
  Encodes a ROUTE message.

  For Bolt 4.3, the format is: (routing_context, bookmarks, database)
  For Bolt 4.4+/5.x, the format is: (routing_context, bookmarks, db_context_map)

  Parameters:
  - routing_context: Map with additional routing context (usually empty %{})
  - bookmarks: List of transaction bookmarks for causal consistency
  - database: Target database name (nil for default)
  """
  @spec encode(float(), map(), list(), String.t() | nil) :: binary()
  def encode(bolt_version, routing_context, bookmarks, database)
      when is_float(bolt_version) and bolt_version >= 4.4 do
    # Bolt 4.4+ / 5.x format: the third parameter is a map
    db_context =
      case database do
        nil -> %{}
        db -> %{"db" => db}
      end

    message = [
      routing_context || %{},
      bookmarks || [],
      db_context
    ]

    MessageEncoder.encode(@signature, message)
  end

  def encode(bolt_version, routing_context, bookmarks, database)
      when is_float(bolt_version) and bolt_version >= 4.3 do
    # Bolt 4.3 format: the third parameter is a string or nil
    message = [
      routing_context || %{},
      bookmarks || [],
      database
    ]

    MessageEncoder.encode(@signature, message)
  end

  @doc """
  Checks if native ROUTE message is supported for the given Bolt version.
  """
  @spec supported?(float()) :: boolean()
  def supported?(bolt_version) when is_float(bolt_version) do
    bolt_version >= 4.3
  end

  def supported?(_), do: false

  @doc """
  Prepares the routing table response.

  The response contains:
  - "rt": The routing table with servers and TTL
  """
  @spec prepare_messages(float(), list()) :: {:ok, map()} | {:error, Boltx.Error.t()}
  def prepare_messages(_bolt_version, messages) do
    case hd(messages) do
      {:success, response} ->
        # The routing table is in the "rt" key of the SUCCESS response
        routing_table = Map.get(response, "rt", response)
        {:ok, routing_table}

      {:failure, response} ->
        {:error,
         Boltx.Error.wrap(__MODULE__, %{
           code: response["code"],
           message: response["message"]
         })}
    end
  end
end
