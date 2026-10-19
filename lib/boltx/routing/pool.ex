defmodule Boltx.Routing.Pool do
  @moduledoc """
  A routing-aware connection pool for Neo4j clusters.

  - Read queries go to followers and write queries to the leader.
  - `NotALeader` and other retryable cluster errors are retried on another server.
  - Routing tables are refreshed when they expire.

  ## Usage

      {:ok, pool} = Boltx.Routing.Pool.start_link(
        uri: "neo4j+s://cluster.example.com:7687",
        auth: [username: "neo4j", password: "password"]
      )

      {:ok, result} = Boltx.Routing.Pool.query(pool, "MATCH (n) RETURN n", %{}, mode: :read)
      {:ok, result} = Boltx.Routing.Pool.query(pool, "CREATE (n:Test) RETURN n", %{})

      {:ok, :saved, bookmark} =
        Boltx.Routing.Pool.transaction(pool, fn tx ->
          {:ok, _} = Boltx.Routing.Pool.query(tx, "CREATE (:Order {id: 1})")
          {:ok, _} = Boltx.Routing.Pool.query(tx, "CREATE (:Item {order: 1})")
          :saved
        end)

  ## How it stays up

  The pool process only answers "which server, which connection pool" from
  memory. The query itself — and any retry — runs in the caller, so queries run
  concurrently, up to `:pool_size` per server, and a slow query never holds
  anyone else up.

  Routing tables are fetched by a supervised task with a hard deadline
  (`:routing_timeout` per router tried). Concurrent requests for the same
  database share one refresh. An expired table that still has servers for the
  requested mode keeps serving while it is refreshed in the background; only a
  request with no usable server waits, and it gets `{:error, %Boltx.Error{}}` —
  never an exit — if the refresh fails or overruns.

  Per-server connection pools live under a supervisor of their own. One that
  dies is started again on its next use; the routing pool itself is unaffected.

  ## Transactions

  `transaction/3` runs a function in one transaction on the leader: all of its
  writes commit together or none do. **The function may run up to
  `:max_retries` times** (a leader change, a deadlock: the server rolled the
  attempt back), so it must only touch the graph — no messages, no other
  database. A deadline bounds the whole of it: waiting for a connection,
  BEGIN, the function and COMMIT. Only a COMMIT whose answer never came is
  reported as possibly saved (`:commit_unknown`); that one is never retried.
  """

  use GenServer
  require Logger

  alias Boltx.Error
  alias Boltx.Routing.{Router, Table, Transaction}

  @default_pool_size 10
  @default_max_retries 3
  @default_retry_delay 100
  @default_routing_timeout 5_000
  @default_timeout 30_000

  # Headroom on top of the per-router attempts before a refresh is abandoned.
  @refresh_slack 500

  # A failed background refresh is not retried sooner than this, so a stale
  # table under load does not hammer a struggling router.
  @refresh_backoff 1_000

  defstruct [
    :config,
    :initial_address,
    :database,
    :servers_sup,
    :tasks_sup,
    router: Router,
    routing_context: %{},
    pool_size: @default_pool_size,
    max_retries: @default_max_retries,
    retry_delay: @default_retry_delay,
    routing_timeout: @default_routing_timeout,
    # database => Table.t()
    tables: %{},
    # address => connection pool pid
    servers: %{},
    # monitor ref => address
    monitors: %{},
    # database => %{task: Task.t(), timer: reference(), waiters: [{from, kind}]}
    refreshes: %{},
    # database => monotonic ms of the last failed refresh
    failed_at: %{}
  ]

  # DBConnection options that should be forwarded to per-server pools
  @forwarded_db_connection_opts [
    :idle_interval,
    :idle_limit,
    :backoff_type,
    :backoff_min,
    :backoff_max,
    :queue_target,
    :queue_interval,
    :queue_timeout,
    :ownership_timeout,
    :connect_timeout,
    :timeout,
    :pool_timeout,
    :after_connect_timeout
  ]

  # Boltx connection options that should be forwarded to per-server pools
  @forwarded_boltx_opts [
    :ssl,
    :ssl_opts,
    :user_agent,
    :versions,
    :socket_options
  ]

  # Public API

  @doc """
  Starts a routing pool.

  ## Options

  - `:uri` - The Neo4j cluster URI (required)
  - `:auth` - Authentication options `[username: ..., password: ...]` (required)
  - `:pool_size` - Number of connections per server (default: #{@default_pool_size})
  - `:database` - Default database name (default: nil for default database)
  - `:max_retries` - Maximum attempts for retryable cluster errors (default: #{@default_max_retries})
  - `:retry_delay` - Delay between retries in ms (default: #{@default_retry_delay})
  - `:routing_timeout` - Deadline in ms for fetching the routing table from one
    router (default: #{@default_routing_timeout})
  - `:routing_context` - Routing context sent with ROUTE (default: `%{}`)
  - `:name` - GenServer name (optional)
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @doc """
  Executes a Cypher query on the cluster.

  ## Options

  - `:mode` - Access mode, `:read` or `:write` (default: `:write`)
  - `:database` - Target database (default: pool's default database)
  - `:bookmarks` - Transaction bookmarks for causal consistency
  - `:timeout` - Query timeout in milliseconds (default: #{@default_timeout})
  """
  @spec query(GenServer.server() | Transaction.t(), String.t(), map(), keyword()) ::
          {:ok, Boltx.Response.t()} | {:error, Error.t() | term()}
  def query(pool_or_transaction, statement, params \\ %{}, opts \\ [])

  # Inside a transaction: the leader connection BEGIN chose. A retryable error
  # rolls the attempt back for `transaction/3` to run again, so the function
  # never sees it. After any other error the transaction refuses every
  # statement (`:transaction_failed`) and can only roll back.
  def query(%Transaction{conn: conn, ref: ref}, statement, params, opts) do
    case DBConnection.execute(conn, %Boltx.Query{statement: statement, extra: %{}}, params, opts) do
      {:ok, _query, result} ->
        {:ok, result}

      {:error, %Error{} = error} ->
        if Error.retryable_transaction_error?(error),
          do: DBConnection.rollback(conn, {__MODULE__, :retry, error})

        if Process.get({Transaction, ref, :error}) == nil,
          do: Process.put({Transaction, ref, :error}, error)

        {:error, error}

      {:error, _reason} = error ->
        error
    end
  end

  def query(pool, statement, params, opts) do
    run(pool, statement, params, opts, Keyword.get(opts, :mode, :write), 0)
  end

  @doc """
  Executes a Cypher query, raising on error.
  """
  @spec query!(GenServer.server(), String.t(), map(), keyword()) :: Boltx.Response.t()
  def query!(pool, statement, params \\ %{}, opts \\ []) do
    case query(pool, statement, params, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @doc """
  Runs `fun` in one transaction on the leader and commits what it returns.

  Read the moduledoc's "Transactions" first: `fun` may run more than once.

  ## Options

  - `:database` - Target database (default: pool's default database)
  - `:bookmarks` - Bookmarks BEGIN waits for (causal consistency)
  - `:deadline` - Absolute `System.monotonic_time(:millisecond)` by which the
    transaction must be over; the server is told the time left (`tx_timeout`)
  - `:timeout` - Used when there is no `:deadline` (default: #{@default_timeout})

  ## Returns

  - `{:ok, result, bookmark}` once committed (`bookmark` may be `nil`)
  - `{:error, reason}` from `rollback/2`
  - `{:error, error}` when it did not commit: the first error a statement
    returned, or BEGIN's or COMMIT's
  - `{:error, %Boltx.Error{code: :commit_unknown}}` when COMMIT's answer was
    lost: it may have committed

  A raise in `fun` rolls back and is raised again.
  """
  @spec transaction(GenServer.server(), (Transaction.t() -> result), keyword()) ::
          {:ok, result, String.t() | nil} | {:error, term()}
        when result: var
  def transaction(pool, fun, opts \\ []) do
    deadline =
      Keyword.get_lazy(opts, :deadline, fn ->
        now() + Keyword.get(opts, :timeout, @default_timeout)
      end)

    run_transaction(pool, fun, opts, deadline, 0)
  end

  @doc """
  Rolls back the transaction: `transaction/3` returns `{:error, reason}`.
  """
  @spec rollback(Transaction.t(), term()) :: no_return()
  def rollback(%Transaction{conn: conn}, reason), do: DBConnection.rollback(conn, reason)

  @doc """
  Gets the current routing table for a database.
  """
  @spec routing_table(GenServer.server(), String.t() | nil) ::
          {:ok, Table.t()} | {:error, term()}
  def routing_table(pool, database \\ nil) do
    call(pool, {:routing_table, database}, @default_timeout)
  end

  @doc """
  Stops the routing pool.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(pool) do
    GenServer.stop(pool)
  end

  # Caller side: routing, execution and retries all run in the calling process.

  defp run(pool, statement, params, opts, mode, attempt) do
    request = {:route, Keyword.get(opts, :database), mode}

    with {:ok, target} <- call(pool, request, Keyword.get(opts, :timeout, @default_timeout)) do
      case execute(target, statement, params, opts, mode) do
        {:retry, error, notice} ->
          GenServer.cast(pool, notice)

          if attempt + 1 < target.max_retries do
            Process.sleep(target.retry_delay)
            run(pool, statement, params, opts, mode, attempt + 1)
          else
            {:error, error}
          end

        result ->
          result
      end
    end
  end

  defp execute(target, statement, params, opts, mode) do
    query = %Boltx.Query{
      statement: statement,
      extra: %{
        mode: if(mode == :read, do: "r", else: "w"),
        db: target.database,
        bookmarks: Keyword.get(opts, :bookmarks, [])
      }
    }

    case DBConnection.execute(target.conn, query, params, opts) do
      {:ok, _query, result} ->
        {:ok, result}

      {:error, %Error{} = error} ->
        cond do
          Error.not_a_leader?(error) ->
            {:retry, error, {:write_failure, target.address, target.database}}

          Error.retryable_cluster_error?(error) ->
            {:retry, error, {:deactivate, target.address, target.database}}

          true ->
            {:error, error}
        end

      {:error, _reason} = error ->
        error
    end
  catch
    # The server's connection pool died between routing and checkout.
    :exit, {_reason, {DBConnection.Holder, :checkout, _args}} ->
      {:retry, Error.wrap(__MODULE__, :server_unavailable),
       {:deactivate, target.address, target.database}}
  end

  defp run_transaction(pool, fun, opts, deadline, attempt) do
    request = {:route, Keyword.get(opts, :database), :write}

    with {:ok, target} <- call(pool, request, max(deadline - now(), 0)) do
      case attempt_transaction(target, fun, opts, deadline) do
        {:retry, error, notice} ->
          if notice, do: GenServer.cast(pool, notice)

          if attempt + 1 < target.max_retries and now() + target.retry_delay < deadline do
            Process.sleep(target.retry_delay)
            run_transaction(pool, fun, opts, deadline, attempt + 1)
          else
            {:error, error}
          end

        result ->
          result
      end
    end
  end

  defp attempt_transaction(target, fun, opts, deadline) do
    ref = make_ref()
    tx = %Transaction{address: target.address, database: target.database, ref: ref}

    begin = %{
      mode: "w",
      db: target.database,
      bookmarks: Keyword.get(opts, :bookmarks, []),
      tx_timeout: max(deadline - now(), 1)
    }

    # Set once `fun` has returned: an error after it is COMMIT's.
    body = fn conn ->
      result = fun.(%{tx | conn: conn})
      Process.put({Transaction, ref}, :body_done)
      result
    end

    Process.delete({Boltx.Connection, :bookmark})

    try do
      outcome =
        try do
          DBConnection.transaction(target.conn, body,
            deadline: deadline,
            timeout: :infinity,
            extra_parameters: begin
          )
        rescue
          error in [Error, DBConnection.ConnectionError] -> {:raised, error}
        catch
          # The server's connection pool died between routing and checkout.
          :exit, {_reason, {DBConnection.Holder, :checkout, _args}} -> :checkout_exit
        end

      committing? = Process.get({Transaction, ref}) == :body_done
      classify(outcome, target, committing?, Process.get({Transaction, ref, :error}))
    after
      Process.delete({Transaction, ref})
      Process.delete({Transaction, ref, :error})
      Process.delete({Boltx.Connection, :bookmark})
    end
  end

  defp classify({:ok, result}, _target, _committing?, _failed),
    do: {:ok, result, Process.get({Boltx.Connection, :bookmark})}

  defp classify({:error, {__MODULE__, :retry, error}}, target, _committing?, _failed),
    do: {:retry, error, notice(error, target)}

  # COMMIT refused because a statement failed: that statement's error says why.
  defp classify({:error, :rollback}, _target, _committing?, %Error{} = failed),
    do: {:error, failed}

  defp classify({:error, _reason} = result, _target, _committing?, _failed), do: result

  # The server answered BEGIN or COMMIT with a FAILURE: nothing was committed.
  defp classify({:raised, %Error{bolt: %{code: "Neo." <> _}} = error}, target, _committing?, _) do
    if Error.retryable_transaction_error?(error),
      do: {:retry, error, notice(error, target)},
      else: {:error, error}
  end

  # A statement already failed, so the server's transaction was gone (RESET
  # or a dead socket) before COMMIT: nothing was committed.
  defp classify({:raised, _error}, _target, true, %Error{} = failed), do: {:error, failed}

  # The connection failed with COMMIT in flight: it may have committed.
  defp classify({:raised, error}, _target, true, _failed) do
    {:error,
     %Error{
       module: __MODULE__,
       code: :commit_unknown,
       bolt: %{code: "Boltx.CommitUnknown", message: Exception.message(error)}
     }}
  end

  defp classify({:raised, error}, _target, false, _failed), do: {:error, error}

  defp classify(:checkout_exit, target, _committing?, _failed) do
    {:retry, Error.wrap(__MODULE__, :server_unavailable),
     {:deactivate, target.address, target.database}}
  end

  defp notice(error, target) do
    cond do
      Error.not_a_leader?(error) -> {:write_failure, target.address, target.database}
      Error.retryable_cluster_error?(error) -> {:deactivate, target.address, target.database}
      true -> nil
    end
  end

  # A GenServer call that cannot exit: a stalled or missing pool is an error
  # the caller can handle, not a crash of the calling process.
  defp call(pool, request, timeout) do
    GenServer.call(pool, request, timeout)
  catch
    :exit, {:timeout, _} -> {:error, Error.wrap(__MODULE__, :routing_timeout)}
    :exit, {reason, _} when reason in [:noproc, :normal, :shutdown] -> pool_down_error()
    :exit, {{:shutdown, _}, _} -> pool_down_error()
    :exit, _reason -> {:error, Error.wrap(__MODULE__, :routing_unavailable)}
  end

  defp pool_down_error, do: {:error, Error.wrap(__MODULE__, :pool_not_running)}

  # GenServer callbacks

  @impl true
  def init(opts) do
    # Supervisors are linked: if either dies, so does the pool, and its own
    # supervisor restarts the lot cleanly.
    Process.flag(:trap_exit, true)
    {:ok, servers_sup} = DynamicSupervisor.start_link(strategy: :one_for_one)
    {:ok, tasks_sup} = Task.Supervisor.start_link()

    uri = Keyword.fetch!(opts, :uri)
    parsed_uri = URI.parse(uri)

    # Preserve the full connection config so per-server pools inherit tuning
    config =
      [uri: uri, auth: Keyword.fetch!(opts, :auth)] ++
        Keyword.take(opts, @forwarded_db_connection_opts ++ @forwarded_boltx_opts)

    {:ok,
     %__MODULE__{
       config: config,
       initial_address: "#{parsed_uri.host}:#{parsed_uri.port || 7687}",
       database: Keyword.get(opts, :database),
       servers_sup: servers_sup,
       tasks_sup: tasks_sup,
       router: Keyword.get(opts, :router, Router),
       routing_context: Keyword.get(opts, :routing_context, %{}),
       pool_size: Keyword.get(opts, :pool_size, @default_pool_size),
       max_retries: Keyword.get(opts, :max_retries, @default_max_retries),
       retry_delay: Keyword.get(opts, :retry_delay, @default_retry_delay),
       routing_timeout: Keyword.get(opts, :routing_timeout, @default_routing_timeout)
     }}
  end

  @impl true
  def handle_call({:route, database, mode}, from, state) do
    database = database || state.database
    table = Map.get(state.tables, database)

    case table && Table.servers_for(table, mode) do
      servers when servers in [nil, []] ->
        {:noreply, await_refresh(state, database, {from, {:route, mode}})}

      servers ->
        state = if Table.expired?(table), do: refresh_in_background(state, database), else: state
        {reply, state} = target(state, Enum.random(servers), database)
        {:reply, reply, state}
    end
  end

  def handle_call({:routing_table, database}, from, state) do
    database = database || state.database

    case Map.get(state.tables, database) do
      nil -> {:noreply, await_refresh(state, database, {from, :table})}
      table -> {:reply, {:ok, table}, state}
    end
  end

  @impl true
  def handle_cast({:write_failure, address, database}, state) do
    Logger.debug("Routing: #{address} is no longer the leader for #{inspect(database)}")
    {:noreply, update_table(state, database, &Table.remove_writer(&1, address))}
  end

  def handle_cast({:deactivate, address, database}, state) do
    Logger.debug("Routing: deactivating #{address} for #{inspect(database)}")
    {:noreply, update_table(state, database, &Table.deactivate_server(&1, address))}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    case refresh_by_ref(state, ref) do
      {database, refresh} ->
        Process.demonitor(ref, [:flush])
        Process.cancel_timer(refresh.timer)

        result =
          case result do
            {:ok, %Table{}} -> result
            {:error, _reason} -> result
            other -> {:error, {:unexpected_routing_result, other}}
          end

        {:noreply, finish_refresh(state, database, result)}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case {refresh_by_ref(state, ref), Map.pop(state.monitors, ref)} do
      {{database, refresh}, _} ->
        Process.cancel_timer(refresh.timer)
        {:noreply, finish_refresh(state, database, {:error, {:refresh_crashed, reason}})}

      {nil, {address, monitors}} when is_binary(address) ->
        Logger.debug("Routing: connection pool for #{address} stopped: #{inspect(reason)}")
        {:noreply, %{state | monitors: monitors, servers: Map.delete(state.servers, address)}}

      _unknown ->
        {:noreply, state}
    end
  end

  def handle_info({:refresh_deadline, ref}, state) do
    case refresh_by_ref(state, ref) do
      {database, refresh} ->
        Task.Supervisor.terminate_child(state.tasks_sup, refresh.task.pid)
        Process.demonitor(ref, [:flush])
        {:noreply, finish_refresh(state, database, {:error, :timeout})}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, pid, reason}, %{servers_sup: pid} = state),
    do: {:stop, reason, state}

  def handle_info({:EXIT, pid, reason}, %{tasks_sup: pid} = state),
    do: {:stop, reason, state}

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  # Routing table refresh

  defp await_refresh(state, database, waiter) do
    state = start_refresh(state, database)
    update_in(state.refreshes[database].waiters, &[waiter | &1])
  end

  defp refresh_in_background(state, database) do
    failed_at = Map.get(state.failed_at, database)

    if failed_at && now() - failed_at < @refresh_backoff,
      do: state,
      else: start_refresh(state, database)
  end

  defp start_refresh(%{refreshes: refreshes} = state, database)
       when is_map_key(refreshes, database),
       do: state

  defp start_refresh(state, database) do
    routers = routers_for(state, database)

    task =
      Task.Supervisor.async_nolink(state.tasks_sup, state.router, :fetch_table, [
        routers,
        state.config,
        database,
        state.routing_context,
        state.routing_timeout
      ])

    deadline = state.routing_timeout * length(routers) + @refresh_slack
    timer = Process.send_after(self(), {:refresh_deadline, task.ref}, deadline)

    put_in(state.refreshes[database], %{task: task, timer: timer, waiters: []})
  end

  defp finish_refresh(state, database, result) do
    {%{waiters: waiters}, refreshes} = Map.pop(state.refreshes, database)
    state = %{state | refreshes: refreshes}

    case result do
      {:ok, %Table{} = table} ->
        state =
          %{
            state
            | tables: Map.put(state.tables, database, table),
              failed_at: Map.delete(state.failed_at, database)
          }
          |> stop_unrouted_servers()

        waiters
        |> Enum.reverse()
        |> Enum.reduce(state, fn {from, kind}, acc ->
          {reply, acc} = reply_for(acc, table, database, kind)
          GenServer.reply(from, reply)
          acc
        end)

      {:error, reason} ->
        Logger.warning(
          "Routing: could not refresh the routing table for #{inspect(database)}: " <>
            inspect(reason)
        )

        error = routing_error(reason)
        Enum.each(waiters, fn {from, _kind} -> GenServer.reply(from, {:error, error}) end)
        %{state | failed_at: Map.put(state.failed_at, database, now())}
    end
  end

  defp reply_for(state, table, _database, :table), do: {{:ok, table}, state}

  defp reply_for(state, table, database, {:route, mode}) do
    case Table.servers_for(table, mode) do
      [] -> {{:error, Error.wrap(__MODULE__, no_servers_code(mode))}, state}
      servers -> target(state, Enum.random(servers), database)
    end
  end

  defp no_servers_code(:write), do: :no_writers_available
  defp no_servers_code(:read), do: :no_readers_available

  defp routing_error(reason) do
    %Error{
      module: __MODULE__,
      code: :routing_unavailable,
      bolt: %{code: "Boltx.Routing.Unavailable", message: inspect(reason)}
    }
  end

  defp refresh_by_ref(state, ref) do
    Enum.find(state.refreshes, fn {_database, refresh} -> refresh.task.ref == ref end)
  end

  defp routers_for(state, database) do
    case Map.get(state.tables, database) do
      nil -> [state.initial_address]
      table -> Enum.uniq(table.routers ++ [state.initial_address])
    end
  end

  defp update_table(state, database, fun) do
    case Map.get(state.tables, database) do
      nil -> state
      table -> %{state | tables: Map.put(state.tables, database, fun.(table))}
    end
  end

  # Per-server connection pools

  defp target(state, address, database) do
    case server_pool(state, address) do
      {:ok, conn, state} ->
        {{:ok,
          %{
            conn: conn,
            address: address,
            database: database,
            max_retries: state.max_retries,
            retry_delay: state.retry_delay
          }}, state}

      {:error, reason} ->
        Logger.warning(
          "Routing: could not start a connection pool for #{address}: #{inspect(reason)}"
        )

        {{:error, Error.wrap(__MODULE__, :server_unavailable)},
         update_table(state, database, &Table.deactivate_server(&1, address))}
    end
  end

  defp server_pool(state, address) do
    case Map.fetch(state.servers, address) do
      {:ok, pid} ->
        {:ok, pid, state}

      :error ->
        config =
          address
          |> Router.server_config(state.config)
          |> Keyword.put(:pool_size, state.pool_size)

        spec = %{id: address, start: {Boltx, :start_link, [config]}, restart: :temporary}

        with {:ok, pid} <- DynamicSupervisor.start_child(state.servers_sup, spec) do
          Logger.debug("Routing: created connection pool for #{address}")
          ref = Process.monitor(pid)

          {:ok, pid,
           %{
             state
             | servers: Map.put(state.servers, address, pid),
               monitors: Map.put(state.monitors, ref, address)
           }}
        end
    end
  end

  # A server that left the cluster keeps its pool reconnecting forever unless
  # it is stopped once no routing table lists it.
  defp stop_unrouted_servers(state) do
    routed = state.tables |> Map.values() |> Enum.flat_map(&Table.all_servers/1) |> MapSet.new()

    {stale, kept} = Map.split_with(state.servers, fn {address, _} -> address not in routed end)

    {stale_monitors, monitors} =
      Map.split_with(state.monitors, fn {_ref, address} -> is_map_key(stale, address) end)

    # Demonitor first, so a late :DOWN cannot evict a newer pool for the same address.
    Enum.each(stale_monitors, fn {ref, _address} -> Process.demonitor(ref, [:flush]) end)

    Enum.each(stale, fn {_address, pid} ->
      DynamicSupervisor.terminate_child(state.servers_sup, pid)
    end)

    %{state | servers: kept, monitors: monitors}
  end

  defp now, do: System.monotonic_time(:millisecond)
end
