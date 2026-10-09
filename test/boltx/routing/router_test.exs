defmodule Boltx.Routing.RouterTest do
  use ExUnit.Case, async: true

  alias Boltx.Routing.{Router, Table}
  alias Boltx.Test.FakeRouter

  @moduletag :routing

  defp config(address) do
    [uri: "neo4j://#{address}", auth: [username: "neo4j", password: "boltxPassword"]]
  end

  defp real_address, do: "127.0.0.1:#{System.get_env("BOLT_TCP_PORT", "7687")}"

  defp timed(fun) do
    {micros, result} = :timer.tc(fun)
    {div(micros, 1000), result}
  end

  describe "fetch_table/5" do
    test "returns the routing table of a real server" do
      assert {:ok, %Table{writers: [_ | _], readers: [_ | _], routers: [_ | _]}} =
               Router.fetch_table([real_address()], config(real_address()), nil, %{}, 5_000)
    end

    test "gives up on a router that accepts the connection and never speaks" do
      router = FakeRouter.start(:silent)

      {ms, result} = timed(fn -> Router.fetch_table([router], config(router), nil, %{}, 300) end)

      assert {:error, _} = result
      assert ms < 1_500
      assert_receive {:fake_router_closed, ^router}, 1_000
    end

    test "gives up on a router that stalls after the handshake, and releases its socket" do
      # The production hang: HELLO is awaited with an :infinity timeout.
      router = FakeRouter.start(:mute_after_handshake)

      {ms, result} = timed(fn -> Router.fetch_table([router], config(router), nil, %{}, 300) end)

      assert {:error, :timeout} = result
      assert ms < 1_500
      assert_receive {:fake_router_closed, ^router}, 1_000
    end

    test "falls through to the next router" do
      stalled = FakeRouter.start(:mute_after_handshake)

      assert {:ok, %Table{writers: [_ | _]}} =
               Router.fetch_table([stalled, real_address()], config(stalled), nil, %{}, 300)
    end

    test "reports the last failure when every router fails" do
      silent = FakeRouter.start(:silent)
      assert {:error, _} = Router.fetch_table([silent], config(silent), nil, %{}, 100)

      assert {:error, :no_available_routers} =
               Router.fetch_table([], config(silent), nil, %{}, 100)
    end
  end

  describe "server_config/2" do
    test "targets the given address, not the seed URI host" do
      config = [uri: "neo4j+s://seed.example.com:7687", auth: [username: "u"], pool_size: 3]

      server = Router.server_config("member-2.example.com:7688", config)

      refute Keyword.has_key?(server, :uri)
      assert server[:hostname] == "member-2.example.com"
      assert server[:port] == 7688
      assert server[:scheme] == "neo4j+s"
      assert server[:auth] == [username: "u"]
      assert server[:pool_size] == 3
    end
  end
end
