defmodule Boltx.Routing.ClusterTest do
  # Needs the 3-primary cluster of docker-compose.cluster.yml (see there);
  # excluded unless `--only cluster`. Not async: it reads the cluster's table.
  use ExUnit.Case, async: false

  alias Boltx.Error
  alias Boltx.Routing.{Pool, Router}

  @moduletag :cluster

  @auth [username: "neo4j", password: "boltxPassword"]

  # Names a follower as the only writer until told otherwise, as a table does
  # for a moment after the leader changes.
  defmodule WrongWriterRouter do
    def fetch_table(addresses, config, database, %{agent: agent}, timeout) do
      with {:ok, table} <- Router.fetch_table(addresses, config, database, %{}, timeout) do
        case Agent.get(agent, & &1) do
          :wrong -> {:ok, %{table | writers: [hd(table.readers -- table.writers)]}}
          :real -> {:ok, table}
        end
      end
    end
  end

  defp seed, do: "neo4j://localhost:#{System.get_env("BOLT_CLUSTER_PORT", "7701")}"

  setup do
    label = "ClusterTx#{System.unique_integer([:positive])}"
    pool = start_supervised!({Pool, uri: seed(), auth: @auth, pool_size: 2})
    on_exit(fn -> cleanup(label) end)
    %{label: label, pool: pool}
  end

  defp cleanup(label) do
    {:ok, pool} = Pool.start_link(uri: seed(), auth: @auth, pool_size: 1)
    Pool.query(pool, "MATCH (n:#{label}) DETACH DELETE n")
    Pool.stop(pool)
  end

  test "a follower refuses a write with NotALeader, which is retryable", %{pool: pool} do
    {:ok, table} = Pool.routing_table(pool)
    [host, port] = String.split(hd(table.readers -- table.writers), ":")

    {:ok, follower} =
      Boltx.start_link(
        hostname: host,
        port: String.to_integer(port),
        scheme: "bolt",
        ssl: false,
        auth: @auth,
        pool_size: 1
      )

    assert {:error, %Error{code: :not_a_leader} = error} =
             Boltx.query(follower, "CREATE (:ClusterTxNever)")

    assert error.bolt.code == "Neo.ClientError.Cluster.NotALeader"
    assert Error.retryable_transaction_error?(error)
    assert Error.write_failure_error?(error)
  end

  test "a transaction sent to a follower runs again on the leader and commits", %{label: l} do
    {:ok, agent} = Agent.start_link(fn -> :wrong end)

    pool =
      start_supervised!(
        {Pool,
         uri: seed(),
         auth: @auth,
         pool_size: 2,
         router: WrongWriterRouter,
         routing_context: %{agent: agent}},
        id: :wrong_writer
      )

    {:ok, %{writers: [follower]}} = Pool.routing_table(pool)
    Agent.update(agent, fn _ -> :real end)
    runs = :counters.new(1, [])

    assert {:ok, :saved, bookmark} =
             Pool.transaction(pool, fn tx ->
               :counters.add(runs, 1, 1)
               {:ok, _} = Pool.query(tx, "CREATE (:#{l} {n: 1})")
               {:ok, _} = Pool.query(tx, "CREATE (:#{l} {n: 2})")
               :saved
             end)

    assert :counters.get(runs, 1) == 2
    {:ok, %{writers: [leader]}} = Pool.routing_table(pool)
    assert leader != follower

    assert {:ok, %{results: [%{"c" => 2}]}} =
             Pool.query(pool, "MATCH (n:#{l}) RETURN count(n) AS c", %{},
               mode: :read,
               bookmarks: [bookmark]
             )
  end

  test "reads after a commit wait for its bookmark on every follower", %{pool: pool, label: l} do
    for i <- 1..20 do
      {:ok, :ok, bookmark} =
        Pool.transaction(pool, fn tx ->
          {:ok, _} = Pool.query(tx, "CREATE (:#{l} {i: $i})", %{i: i})
          :ok
        end)

      assert {:ok, %{results: [%{"c" => ^i}]}} =
               Pool.query(pool, "MATCH (n:#{l}) RETURN count(n) AS c", %{},
                 mode: :read,
                 bookmarks: [bookmark]
               )
    end
  end
end
