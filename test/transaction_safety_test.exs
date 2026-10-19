defmodule Transaction.SafetyTest do
  # Not async: other suites wipe the shared database (`MATCH (n) DETACH DELETE n`),
  # and these tests count what a transaction left behind.
  use ExUnit.Case, async: false

  alias Boltx.Response

  @opts Boltx.TestHelper.opts()

  # One connection, so every test also proves the next checkout gets a clean
  # connection back. Each test has its own label.
  describe "a transaction that fails" do
    setup do
      {:ok, conn} = Boltx.start_link(@opts)
      label = "TxSafe#{System.unique_integer([:positive])}"

      on_exit(fn ->
        {:ok, cleaner} = Boltx.start_link(@opts)
        Boltx.query(cleaner, "MATCH (n:#{label}) DETACH DELETE n")
      end)

      %{conn: conn, label: label}
    end

    @tag :core
    test "refuses every statement after a failed one, and commits nothing", c do
      result =
        Boltx.transaction(c.conn, fn tx ->
          assert {:ok, _} = Boltx.query(tx, "CREATE (:#{c.label} {n: 'a'})")
          assert {:error, %Boltx.Error{}} = Boltx.query(tx, "RETURN 1/0")

          assert {:error, %Boltx.Error{code: :transaction_failed}} =
                   Boltx.query(tx, "CREATE (:#{c.label} {n: 'b'})")

          :returned_normally
        end)

      assert result == {:error, :rollback}
      assert count(c.conn, c.label) == 0
    end

    @tag :core
    test "rolls back on request after a failed statement", c do
      result =
        Boltx.transaction(c.conn, fn tx ->
          Boltx.query!(tx, "CREATE (:#{c.label})")
          {:error, _} = Boltx.query(tx, "RETURN 1/0")
          Boltx.rollback(tx, :boom)
        end)

      assert result == {:error, :boom}
      assert count(c.conn, c.label) == 0
      assert {:ok, _} = Boltx.query(c.conn, "RETURN 1")
    end

    @tag :core
    test "re-raises what the function raised, and commits nothing", c do
      assert_raise RuntimeError, "boom", fn ->
        Boltx.transaction(c.conn, fn tx ->
          Boltx.query!(tx, "CREATE (:#{c.label})")
          raise "boom"
        end)
      end

      assert count(c.conn, c.label) == 0
    end

    @tag :core
    test "a caller killed mid-transaction commits nothing", c do
      parent = self()

      pid =
        spawn(fn ->
          Boltx.transaction(c.conn, fn tx ->
            Boltx.query!(tx, "CREATE (:#{c.label})")
            send(parent, :written)
            Process.sleep(:infinity)
          end)
        end)

      assert_receive :written, 5_000
      Process.exit(pid, :kill)

      assert count(c.conn, c.label) == 0
    end

    @tag :core
    test "the deadline ends a transaction stuck on a lock", c do
      {:ok, other} = Boltx.start_link(@opts)
      Boltx.query!(c.conn, "CREATE (:#{c.label} {k: 1})")
      parent = self()

      holder =
        Task.async(fn ->
          Boltx.transaction(other, fn tx ->
            Boltx.query!(tx, "MATCH (n:#{c.label}) SET n.k = 2")
            send(parent, :locked)

            receive do
              :release -> Boltx.rollback(tx, :released)
            end
          end)
        end)

      assert_receive :locked, 5_000
      deadline = System.monotonic_time(:millisecond) + 500

      {micros, result} =
        :timer.tc(fn ->
          attempt(fn ->
            Boltx.transaction(
              c.conn,
              fn tx -> Boltx.query(tx, "MATCH (n:#{c.label}) SET n.k = 3") end,
              deadline: deadline
            )
          end)
        end)

      refute match?({:ok, {:ok, _}}, result)
      assert micros < 2_000_000

      send(holder.pid, :release)
      assert {:error, :released} = Task.await(holder)

      {answer_micros, {:ok, _}} = :timer.tc(fn -> Boltx.query(c.conn, "RETURN 1") end)
      assert answer_micros < 1_000_000

      assert %Response{results: [%{"k" => 1}]} =
               Boltx.query!(c.conn, "MATCH (n:#{c.label}) RETURN n.k AS k")
    end

    @tag :core
    test "the connection reports its transaction status", c do
      assert {:ok, :transaction} =
               Boltx.transaction(c.conn, fn tx -> DBConnection.status(tx) end)

      assert DBConnection.status(c.conn) == :idle

      assert {:error, :x} =
               Boltx.transaction(c.conn, fn tx ->
                 {:error, _} = Boltx.query(tx, "RETURN 1/0")
                 assert DBConnection.status(tx) == :error
                 Boltx.rollback(tx, :x)
               end)

      assert DBConnection.status(c.conn) == :idle
    end

    @tag :core
    test "the same connection begins and commits again after every failure", c do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          # BEGIN refused (a bookmark the server never issued)
          assert_raise Boltx.Error, fn ->
            Boltx.transaction(c.conn, fn _tx -> :never end, [], %{bookmarks: ["bogus"]})
          end

          assert_commits(c)

          # COMMIT refused: the server timed the transaction out (its monitor
          # runs every 2 s)
          error =
            assert_raise Boltx.Error, fn ->
              Boltx.transaction(c.conn, fn _tx -> Process.sleep(2_500) end, [], %{tx_timeout: 100})
            end

          send(self(), {:commit_error, error})
          assert_commits(c)

          # rollback of a failed transaction (no wire)
          assert {:error, :rollback} =
                   Boltx.transaction(c.conn, fn tx -> Boltx.query(tx, "RETURN 1/0") end)

          assert_commits(c)

          # a raise inside the function
          assert_raise RuntimeError, fn -> Boltx.transaction(c.conn, fn _ -> raise "x" end) end
          assert_commits(c)
        end)

      refute log =~ "disconnected"
      assert_received {:commit_error, %Boltx.Error{code: :transaction_timed_out}}
      assert count(c.conn, c.label) == 4
    end
  end

  defp assert_commits(c) do
    assert {:ok, _} =
             Boltx.transaction(c.conn, fn tx -> Boltx.query!(tx, "CREATE (:#{c.label})") end)
  end

  defp count(conn, label) do
    %Response{results: [%{"c" => count}]} =
      Boltx.query!(conn, "MATCH (n:#{label}) RETURN count(n) AS c")

    count
  end

  defp attempt(fun) do
    {:ok, fun.()}
  rescue
    error -> {:raised, error}
  catch
    :exit, reason -> {:exit, reason}
  end
end
