defmodule Boltx.Test.FakeRouter do
  @moduledoc """
  TCP servers that misbehave like a stalled Neo4j router.

  - `:silent` accepts the connection and never sends a byte.
  - `:mute_after_handshake` negotiates Bolt 5.4, then never answers HELLO —
    the client waits on that reply with an `:infinity` timeout, which is how a
    production router stall used to hang the routing pool forever.

  Every closed connection is reported to the starting process as
  `{:fake_router_closed, address}`, so a test can prove the socket was released.
  """

  @spec start(:silent | :mute_after_handshake) :: String.t()
  def start(behaviour) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    address = "127.0.0.1:#{port}"
    owner = self()

    spawn_link(fn -> accept_loop(listen, behaviour, address, owner) end)
    address
  end

  defp accept_loop(listen, behaviour, address, owner) do
    case :gen_tcp.accept(listen) do
      {:ok, sock} ->
        handler = spawn(fn -> handle(behaviour, address, owner) end)
        :ok = :gen_tcp.controlling_process(sock, handler)
        send(handler, {:socket, sock})
        accept_loop(listen, behaviour, address, owner)

      {:error, _} ->
        :ok
    end
  end

  defp handle(behaviour, address, owner) do
    receive do
      {:socket, sock} ->
        if behaviour == :mute_after_handshake do
          {:ok, _handshake} = :gen_tcp.recv(sock, 20)
          :ok = :gen_tcp.send(sock, <<0, 0, 4, 5>>)
        end

        drain_until_closed(sock)
        send(owner, {:fake_router_closed, address})
    end
  end

  defp drain_until_closed(sock) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, _data} -> drain_until_closed(sock)
      {:error, _} -> :ok
    end
  end
end
