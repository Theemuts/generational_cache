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

    {:ok, %{tables: tables, shards: shards}}
  end

  def handle_call(:drop_cold_cache, _from, %{shards: shards, tables: tables} = s) do
    Enum.map(0..shards-1, fn(shard) ->
      pool = Util.get_pool_name(shard)
      tables = Map.fetch!(tables, shard)
      :ok = lock_shard(pool)
      :ok = do_drop(tables)
      :ok = unlock_shard(pool)
    end)

    {:reply, :ok, s}
  end

  defp lock_shard(pool) do
    SpaghettiPool.lock(pool)
  end

  defp unlock_shard(pool) do
    SpaghettiPool.unlock(pool)
  end

  defp do_drop({hot, cold, waiting}) do
    waiting = :ets.rename(cold, waiting) # Cold table is now the waiting table
    ^cold = :ets.rename(hot, cold) # Hot table is now the cold table
    pid = Process.whereis(GenerationalCache.TableManager)
    ^hot = :ets.new(hot, [:named_table, :public, :set, read_concurrency: true]) # Hot table is newly created
    :ets.give_away(hot, pid, [])
    Task.start(fn -> :ets.delete(waiting) end) # Delete old cold cache asynchronously.
    :ok
  end
end