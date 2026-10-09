defmodule Boltx.Routing.Router do
  @moduledoc """
  Fetches a cluster routing table from one of its routers.

  Stateless: the routing state lives in `Boltx.Routing.Pool`. Every attempt
  runs in its own process with a hard deadline, because the Bolt client waits
  for the HELLO / LOGON / ROUTE replies with an `:infinity` timeout — a router
  that accepts the connection and then goes silent would otherwise block its
  caller forever. Killing the attempt process closes its socket.
  """

  require Logger

  alias Boltx.Client
  alias Boltx.Routing.Table

  @doc """
  Tries `addresses` in order and returns the first routing table obtained.

  `config` is the pool's connection config (it carries `:uri`, `:auth` and
  the TLS options). Each attempt is bounded by `attempt_timeout` ms.
  """
  @spec fetch_table([String.t()], keyword(), String.t() | nil, map(), timeout()) ::
          {:ok, Table.t()} | {:error, term()}
  def fetch_table(addresses, config, database, routing_context, attempt_timeout) do
    Enum.reduce_while(addresses, {:error, :no_available_routers}, fn address, _last ->
      case attempt(address, config, database, routing_context, attempt_timeout) do
        {:ok, _table} = ok ->
          {:halt, ok}

        {:error, reason} = error ->
          Logger.debug("Routing: failed to fetch table from #{address}: #{inspect(reason)}")
          {:cont, error}
      end
    end)
  end

  defp attempt(address, config, database, routing_context, timeout) do
    task = Task.async(fn -> fetch_from(address, config, database, routing_context, timeout) end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:exit, reason}}
      nil -> {:error, :timeout}
    end
  end

  defp fetch_from(address, config, database, routing_context, timeout) do
    server_config = address |> server_config(config) |> Keyword.put(:connect_timeout, timeout)

    with {:ok, client} <- Client.connect(server_config) do
      try do
        with {:ok, _} <- init_connection(client, server_config),
             {:ok, routing_info} <- Client.send_route(client, routing_context, [], database) do
          {:ok, Table.parse(routing_info, database)}
        end
      after
        Client.disconnect(client)
      end
    end
  rescue
    # A malformed reply is a failed attempt, not a crash: the next router is tried.
    exception -> {:error, {:exception, Exception.message(exception)}}
  end

  @doc """
  Connection options to reach `address` itself.

  `:uri` is dropped: `Boltx.Client.Config` prefers the URI host over
  `:hostname`, so keeping it would send every attempt back to the seed
  address instead of the router being tried.
  """
  @spec server_config(String.t(), keyword()) :: keyword()
  def server_config(address, config) do
    {host, port} = parse_address(address)
    scheme = config |> Keyword.get(:uri) |> to_string() |> URI.parse() |> Map.get(:scheme)

    config
    |> Keyword.delete(:uri)
    |> Keyword.merge(hostname: host, port: port)
    |> then(fn opts -> if scheme, do: Keyword.put(opts, :scheme, scheme), else: opts end)
  end

  @doc """
  Splits `"host:port"` (port defaults to 7687).
  """
  @spec parse_address(String.t()) :: {String.t(), :inet.port_number()}
  def parse_address(address) when is_binary(address) do
    case String.split(address, ":") do
      [host, port] -> {host, String.to_integer(port)}
      [host] -> {host, 7687}
    end
  end

  # HELLO / LOGON depending on the negotiated Bolt version.
  defp init_connection(client, opts) do
    cond do
      client.bolt_version >= 5.1 ->
        with {:ok, _} <- Client.send_hello(client, opts),
             {:ok, _} <- Client.send_logon(client, opts) do
          {:ok, :initialized}
        end

      client.bolt_version >= 3.0 ->
        Client.send_hello(client, opts)

      true ->
        Client.send_init(client, opts)
    end
  end
end
