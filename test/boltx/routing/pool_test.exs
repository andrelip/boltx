defmodule Boltx.Routing.PoolTest do
  # Not async: other suites wipe the shared database (`MATCH (n) DETACH DELETE n`),
  # which would delete the node the lock test blocks on.
  use ExUnit.Case, async: false

  alias Boltx.Error
  alias Boltx.Routing.{Pool, Router}
  alias Boltx.Test.FakeRouter

  @moduletag :routing

  # A router whose behaviour the test switches at runtime. Tables it returns
  # expire at once (ttl 0), so every query also exercises the refresh path.
  defmodule ScriptedRouter do
    def fetch_table(addresses, config, database, %{agent: agent}, timeout) do
      Agent.update(agent, &Map.update!(&1, :calls, fn calls -> calls + 1 end))

      case Agent.get(agent, & &1.mode) do
        :real ->
          with {:ok, table} <- Router.fetch_table(addresses, config, database, %{}, timeout) do
            {:ok, %{table | ttl: 0}}
          end

        :hang ->
          Process.sleep(:infinity)

        :crash ->
          raise "router exploded"

        :garbage ->
          :not_a_table
      end
    end
  end

  defp port, do: System.get_env("BOLT_TCP_PORT", "7687")

  defp pool_opts(extra) do
    [uri: "neo4j://127.0.0.1:#{port()}", auth: [username: "neo4j", password: "boltxPassword"]]
    |> Keyword.merge(pool_size: 5)
    |> Keyword.merge(extra)
  end

  defp start_pool(extra \\ []), do: start_supervised!({Pool, pool_opts(extra)})

  defp scripted_pool(mode, extra \\ []) do
    {:ok, agent} = Agent.start_link(fn -> %{mode: mode, calls: 0} end)
    pool = start_pool([router: ScriptedRouter, routing_context: %{agent: agent}] ++ extra)
    {pool, agent}
  end

  defp set_mode(agent, mode), do: Agent.update(agent, &%{&1 | mode: mode})
  defp calls(agent), do: Agent.get(agent, & &1.calls)

  defp timed(fun) do
    {micros, result} = :timer.tc(fun)
    {div(micros, 1000), result}
  end

  defp read_one(pool, opts \\ []) do
    Pool.query(pool, "RETURN 1 AS n", %{}, [mode: :read] ++ opts)
  end

  describe "queries" do
    test "reads and writes through the cluster routing" do
      pool = start_pool()

      assert {:ok, %{results: [%{"n" => 1}]}} = read_one(pool)

      assert {:ok, %{results: [%{"v" => 7}]}} =
               Pool.query(pool, "CREATE (n:PoolTest {v: $v}) RETURN n.v AS v", %{v: 7})

      Pool.query!(pool, "MATCH (n:PoolTest) DELETE n")
    end

    test "a slow query does not hold up the others" do
      # The pool used to execute every query inside its own process: one query
      # waiting on a lock froze all of them.
      pool = start_pool()
      Pool.query!(pool, "MERGE (n:PoolLock {id: 1})")
      {:ok, conn} = Boltx.start_link(Boltx.TestHelper.opts() ++ [port: String.to_integer(port())])
      test = self()

      holder =
        Task.async(fn ->
          Boltx.transaction(conn, fn tx ->
            Boltx.query!(tx, "MATCH (n:PoolLock {id: 1}) SET n.v = 1")
            send(test, :lock_held)
            Process.sleep(1_500)
          end)
        end)

      assert_receive :lock_held, 2_000
      blocked = Task.async(fn -> Pool.query(pool, "MATCH (n:PoolLock {id: 1}) SET n.v = 2") end)
      Process.sleep(100)

      assert {:ok, _} = read_one(pool)
      # The read finished while the write is still waiting on the lock.
      assert Task.yield(blocked, 0) == nil
      assert {:ok, _} = Task.await(blocked, 5_000)
      Task.await(holder, 5_000)
      Pool.query!(pool, "MATCH (n:PoolLock) DELETE n")
    end

    test "returns a non-retryable server error at once" do
      pool = start_pool()
      {:ok, conn} = Boltx.start_link(Boltx.TestHelper.opts() ++ [port: String.to_integer(port())])
      Boltx.query!(conn, "CREATE DATABASE pooltestro IF NOT EXISTS WAIT", %{}, db: "system")
      Boltx.query!(conn, "ALTER DATABASE pooltestro SET ACCESS READ ONLY WAIT", %{}, db: "system")

      assert {:error,
              %Error{bolt: %{code: "Neo.ClientError.General.WriteOnReadOnlyAccessDatabase"}}} =
               Pool.query(pool, "CREATE (n:X)", %{}, database: "pooltestro")

      assert {:ok, _} = read_one(pool)
    end
  end

  describe "a stalled router" do
    test "fails the query with an error within the routing deadline — no exit, no crash" do
      router = FakeRouter.start(:mute_after_handshake)
      pool = start_pool(uri: "neo4j://#{router}", routing_timeout: 300)

      {ms, result} = timed(fn -> read_one(pool) end)

      assert {:error, %Error{code: :routing_unavailable}} = result
      assert ms < 2_000
      assert Process.alive?(pool)
      assert_receive {:fake_router_closed, ^router}, 2_000
    end

    test "is asked once however many callers are waiting" do
      {pool, agent} = scripted_pool(:hang, routing_timeout: 300)

      results =
        1..20
        |> Enum.map(fn _ -> Task.async(fn -> read_one(pool) end) end)
        |> Task.await_many(5_000)

      assert Enum.all?(results, &match?({:error, %Error{code: :routing_unavailable}}, &1))
      assert calls(agent) == 1
      assert Process.alive?(pool)
    end

    test "does not stop queries while a usable table exists" do
      {pool, agent} = scripted_pool(:real, routing_timeout: 300)
      assert {:ok, _} = read_one(pool)

      set_mode(agent, :hang)

      for _ <- 1..30 do
        {ms, result} = timed(fn -> read_one(pool) end)
        assert {:ok, _} = result
        assert ms < 500
      end

      assert Process.alive?(pool)
    end

    test "is retried after a failure, and the pool recovers" do
      {pool, agent} = scripted_pool(:hang, routing_timeout: 200)
      assert {:error, %Error{code: :routing_unavailable}} = read_one(pool)

      set_mode(agent, :real)

      assert {:ok, _} = read_one(pool)
    end

    test "a caller with a shorter timeout gets an error, not an exit" do
      {pool, _agent} = scripted_pool(:hang, routing_timeout: 5_000)

      {ms, result} = timed(fn -> read_one(pool, timeout: 200) end)

      assert {:error, %Error{code: :routing_timeout}} = result
      assert ms < 1_000
      assert Process.alive?(pool)
    end
  end

  describe "failures inside the pool" do
    test "a crashing router fails the query, not the pool" do
      {pool, _agent} = scripted_pool(:crash)

      assert {:error, %Error{code: :routing_unavailable, bolt: %{message: message}}} =
               read_one(pool)

      assert message =~ "refresh_crashed"
      assert Process.alive?(pool)
    end

    test "an unexpected router result fails the query, not the pool" do
      {pool, _agent} = scripted_pool(:garbage)

      assert {:error, %Error{code: :routing_unavailable}} = read_one(pool)
      assert Process.alive?(pool)
    end

    test "a dead server connection pool is replaced on next use" do
      pool = start_pool()
      assert {:ok, _} = read_one(pool)
      [server] = Map.values(:sys.get_state(pool).servers)

      ref = Process.monitor(server)
      Process.exit(server, :kill)
      assert_receive {:DOWN, ^ref, :process, ^server, :killed}

      assert {:ok, _} = read_one(pool)
      assert Process.alive?(pool)
    end

    test "a server pool that dies between routing and execution is retried elsewhere" do
      # Force the race: the pool hands out a pid that is already dead.
      pool = start_pool()
      assert {:ok, _} = read_one(pool)
      [{address, server}] = Map.to_list(:sys.get_state(pool).servers)

      :sys.suspend(pool)
      Process.exit(server, :kill)
      :sys.replace_state(pool, fn state -> %{state | servers: %{address => server}} end)
      :sys.resume(pool)

      assert {:ok, _} = read_one(pool)
    end

    test "a stopped pool returns an error" do
      pool = start_pool()
      stop_supervised!(Pool)

      assert {:error, %Error{code: :pool_not_running}} = read_one(pool)
    end
  end

  describe "transaction" do
    setup do
      label = "PoolTx#{System.unique_integer([:positive])}"
      on_exit(fn -> cleanup(label) end)
      %{label: label}
    end

    defp cleanup(label) do
      {:ok, conn} = Boltx.start_link(direct_opts())
      Boltx.query(conn, "MATCH (n:#{label}) DETACH DELETE n")
    end

    defp direct_opts, do: Boltx.TestHelper.opts() ++ [port: String.to_integer(port())]

    defp count(pool, label, opts \\ []) do
      {:ok, %{results: [%{"c" => c}]}} =
        Pool.query(pool, "MATCH (n:#{label}) RETURN count(n) AS c", %{}, opts)

      c
    end

    test "commits and returns the bookmark a later read can wait for", %{label: l} do
      pool = start_pool()

      assert {:ok, :saved, bookmark} =
               Pool.transaction(pool, fn tx ->
                 {:ok, _} = Pool.query(tx, "CREATE (:#{l} {n: 1})")
                 {:ok, _} = Pool.query(tx, "CREATE (:#{l} {n: 2})")
                 :saved
               end)

      assert is_binary(bookmark)
      assert count(pool, l, mode: :read, bookmarks: [bookmark]) == 2

      # BEGIN accepts it too
      assert {:ok, 2, _} =
               Pool.transaction(
                 pool,
                 fn tx ->
                   {:ok, %{results: [%{"c" => c}]}} =
                     Pool.query(tx, "MATCH (n:#{l}) RETURN count(n) AS c")

                   c
                 end,
                 bookmarks: [bookmark]
               )
    end

    test "sees its own writes; nobody else does until it commits", %{label: l} do
      pool = start_pool()
      {:ok, other} = Boltx.start_link(direct_opts())

      assert {:ok, :ok, _} =
               Pool.transaction(pool, fn tx ->
                 {:ok, _} = Pool.query(tx, "CREATE (:#{l})")

                 assert {:ok, %{results: [%{"c" => 1}]}} =
                          Pool.query(tx, "MATCH (n:#{l}) RETURN count(n) AS c")

                 assert %{results: [%{"c" => 0}]} =
                          Boltx.query!(other, "MATCH (n:#{l}) RETURN count(n) AS c")

                 :ok
               end)

      assert %{results: [%{"c" => 1}]} =
               Boltx.query!(other, "MATCH (n:#{l}) RETURN count(n) AS c")
    end

    test "rollback/2 leaves nothing and returns its reason", %{label: l} do
      pool = start_pool()

      assert {:error, :changed_my_mind} =
               Pool.transaction(pool, fn tx ->
                 {:ok, _} = Pool.query(tx, "CREATE (:#{l})")
                 Pool.rollback(tx, :changed_my_mind)
               end)

      assert count(pool, l) == 0
    end

    test "a failed statement makes it all fail, even if the function goes on", %{label: l} do
      pool = start_pool()

      result =
        Pool.transaction(pool, fn tx ->
          {:ok, _} = Pool.query(tx, "CREATE (:#{l} {n: 'a'})")
          {:error, _} = Pool.query(tx, "RETURN 1/0")

          assert {:error, %Error{code: :transaction_failed}} =
                   Pool.query(tx, "CREATE (:#{l} {n: 'b'})")

          {:ok, :pretending}
        end)

      assert {:error, %Error{bolt: %{code: "Neo.ClientError.Statement.ArithmeticError"}}} = result
      assert count(pool, l) == 0
      assert {:ok, _} = read_one(pool)
    end

    test "a raise rolls back and is raised again", %{label: l} do
      pool = start_pool()

      assert_raise RuntimeError, "boom", fn ->
        Pool.transaction(pool, fn tx ->
          {:ok, _} = Pool.query(tx, "CREATE (:#{l})")
          raise "boom"
        end)
      end

      assert count(pool, l) == 0
    end

    test "a server pool dead before BEGIN is dropped and the transaction runs again",
         %{label: l} do
      pool = start_pool()
      assert {:ok, _} = read_one(pool)
      [{address, server}] = Map.to_list(:sys.get_state(pool).servers)

      :sys.suspend(pool)
      Process.exit(server, :kill)
      :sys.replace_state(pool, fn state -> %{state | servers: %{address => server}} end)
      :sys.resume(pool)

      runs = :counters.new(1, [])

      assert {:ok, :ok, _} =
               Pool.transaction(pool, fn tx ->
                 :counters.add(runs, 1, 1)
                 {:ok, _} = Pool.query(tx, "CREATE (:#{l})")
                 :ok
               end)

      assert :counters.get(runs, 1) == 1
      assert count(pool, l) == 1
    end

    test "a deadlock is retried: both transactions commit", %{label: l} do
      pool = start_pool()
      {:ok, _} = Pool.query(pool, "CREATE (:#{l} {k: 'a'}), (:#{l} {k: 'b'})")
      test = self()

      run = fn first, second ->
        runs = :counters.new(1, [])

        task =
          Task.async(fn ->
            Pool.transaction(pool, fn tx ->
              :counters.add(runs, 1, 1)
              {:ok, _} = Pool.query(tx, "MATCH (n:#{l} {k: '#{first}'}) SET n.v = 1")

              # Only the first run waits for the other to hold its lock.
              if :counters.get(runs, 1) == 1 do
                send(test, {:locked, first})

                receive do
                  :go -> :ok
                end
              end

              Pool.query(tx, "MATCH (n:#{l} {k: '#{second}'}) SET n.v = 1")
            end)
          end)

        {task, runs}
      end

      {ab, ab_runs} = run.("a", "b")
      {ba, ba_runs} = run.("b", "a")
      assert_receive {:locked, "a"}, 5_000
      assert_receive {:locked, "b"}, 5_000
      send(ab.pid, :go)
      send(ba.pid, :go)

      assert {:ok, {:ok, _}, _} = Task.await(ab, 10_000)
      assert {:ok, {:ok, _}, _} = Task.await(ba, 10_000)
      assert Enum.sort([:counters.get(ab_runs, 1), :counters.get(ba_runs, 1)]) == [1, 2]
    end

    test "50 at once on 5 connections all commit", %{label: l} do
      pool = start_pool()

      results =
        1..50
        |> Enum.map(fn i ->
          Task.async(fn ->
            Pool.transaction(pool, fn tx -> Pool.query(tx, "CREATE (:#{l} {i: $i})", %{i: i}) end)
          end)
        end)
        |> Task.await_many(30_000)

      assert Enum.all?(results, &match?({:ok, {:ok, _}, _}, &1))
      assert count(pool, l) == 50
      assert {:ok, _} = read_one(pool)
    end

    test "a stalled router is an error, not an exit" do
      {pool, _agent} = scripted_pool(:hang, routing_timeout: 5_000)

      {ms, result} =
        timed(fn -> Pool.transaction(pool, fn _tx -> :never end, timeout: 200) end)

      assert {:error, %Error{code: :routing_timeout}} = result
      assert ms < 1_000
      assert Process.alive?(pool)
    end

    test "the deadline ends one stuck on a lock, and nothing commits", %{label: l} do
      pool = start_pool()
      {:ok, _} = Pool.query(pool, "CREATE (:#{l} {v: 0})")
      {:ok, conn} = Boltx.start_link(direct_opts())
      test = self()

      holder =
        Task.async(fn ->
          Boltx.transaction(conn, fn tx ->
            Boltx.query!(tx, "MATCH (n:#{l}) SET n.v = 1")
            send(test, :lock_held)

            receive do
              :release -> Boltx.rollback(tx, :released)
            end
          end)
        end)

      assert_receive :lock_held, 5_000

      {ms, result} =
        timed(fn ->
          Pool.transaction(
            pool,
            fn tx ->
              case Pool.query(tx, "MATCH (n:#{l}) SET n.v = 2") do
                {:ok, _} -> :written
                {:error, error} -> Pool.rollback(tx, error)
              end
            end,
            timeout: 500
          )
        end)

      assert {:error, _} = result
      assert ms < 2_000
      send(holder.pid, :release)
      Task.await(holder, 5_000)

      assert {:ok, %{results: [%{"v" => 0}]}} =
               Pool.query(pool, "MATCH (n:#{l}) RETURN n.v AS v", %{}, mode: :write)
    end

    # Neo4j 5.21 refuses writes to a read-only database with
    # General.WriteOnReadOnlyAccessDatabase: an admin's setting, not a leader
    # change, so it is not retried. (A follower's ForbiddenOnReadOnlyDatabase
    # is, see the cluster test.)
    test "a refusal that is not retryable runs the function once", %{label: l} do
      pool = start_pool()
      {:ok, conn} = Boltx.start_link(direct_opts())
      Boltx.query!(conn, "CREATE DATABASE pooltestro IF NOT EXISTS WAIT", %{}, db: "system")
      Boltx.query!(conn, "ALTER DATABASE pooltestro SET ACCESS READ ONLY WAIT", %{}, db: "system")
      runs = :counters.new(1, [])

      result =
        Pool.transaction(
          pool,
          fn tx ->
            :counters.add(runs, 1, 1)

            case Pool.query(tx, "CREATE (:#{l})") do
              {:ok, _} -> :written
              {:error, error} -> Pool.rollback(tx, error)
            end
          end,
          database: "pooltestro"
        )

      assert {:error,
              %Error{bolt: %{code: "Neo.ClientError.General.WriteOnReadOnlyAccessDatabase"}}} =
               result

      assert :counters.get(runs, 1) == 1
    end
  end

  describe "routing_table/2" do
    test "returns the cached table" do
      pool = start_pool()

      assert {:ok, %Boltx.Routing.Table{writers: [_ | _]}} = Pool.routing_table(pool)
    end
  end
end
