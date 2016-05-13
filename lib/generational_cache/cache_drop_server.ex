defmodule GenerationalCache.CacheDropServer do
  @moduledoc false

  alias GenerationalCache.Util
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def drop_cold_cache do
    GenServer.call(__MODULE__, :drop_cold_cache)
  end

  def init(:ok) do
    shards = Application.get_env(:generational_cache, :shards, 2)

    tables = 0..shards-1
    |> Enum.map(&({&1, Util.get_table_names(&1)}))
    |> Enum.into(%{})

    0..shards-1
    |> Enum.map(&Util.get_pool_name(&1))
    |> Enum.map(&:ets.insert(GenerationalCache.Locks, {&1, 0}))

    {:ok, %{tables: tables, shards: shards}}
  end

  def handle_call(:drop_cold_cache, _from, %{shards: shards, tables: tables} = s) do
    0..shards-1
    |> Enum.map(fn(shard) ->
         pool = Util.get_pool_name(shard)
         tables = Map.fetch!(tables, shard)
         true = lock_shard(pool)
         :ok = await_shard_unaccessed(pool)
         true = do_drop(tables)
         true = unlock_shard(pool)
       end)
    {:reply, :ok, s}
  end

  defp lock_shard(pool) do
    set_lock(pool)
  end

  defp unlock_shard(pool) do
    set_lock(pool, 0)
  end

  defp set_lock(pool, lock \\ 1) do
    :ets.insert(GenerationalCache.Locks, {pool, lock})
  end

  defp await_shard_unaccessed(pool) do
    # TODO: What to do on timeout?
    avail = GenServer.call(pool, :get_avail_workers)
    all = GenServer.call(pool, :get_all_workers)

    case length(all) do
     l when l == length(avail) -> :ok
    _ ->
      :timer.sleep(10)
      await_shard_unaccessed(pool)
    end
  end

  defp do_drop({hot, cold, waiting}) do
    waiting = :ets.rename(cold, waiting) # Cold table is now the waiting table
    ^cold = :ets.rename(hot, cold) # Hot table is now the cold table
    pid = Process.whereis(GenerationalCache.TableManager)
    ^hot = :ets.new(hot, [:named_table, :public, :set, read_concurrency: true]) # Hot table is newly created
    :ets.give_away(hot, pid, [])
    Task.start(fn -> :ets.delete(waiting) end) # Delete old cold cache asynchronously.
    true
  end
end