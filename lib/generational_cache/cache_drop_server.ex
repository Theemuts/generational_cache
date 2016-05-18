defmodule GenerationalCache.CacheDropServer do
  @moduledoc """
  This `GenServer` is called when the maximum size of the cache is exceeded.
  It will lock a shard, rename its tables and delete the cold data, and
  unlock the shard before moving on to the next.

  Currently, this is the only expiry mechanism besides explicit deletion. This
  module is not for public use, calling `drop_cold_cache/0` before a previous
  call has completed will result in a crash.
  """

  alias GenerationalCache.Util
  use GenServer

  @doc false
  @spec start_link :: GenServer.start_link
  def start_link do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Delete all data in the current cold caches, turn the hot caches into the old
  caches and create a new hot caches.
  """
  @spec drop_cold_cache :: true
  def drop_cold_cache do
    GenServer.call(__MODULE__, :drop_cold_cache)
  end

  @doc false
  @spec init(:ok) :: {:ok, map}
  def init(:ok) do
    shards = Application.get_env(:generational_cache, :shards, 2)

    tables = 0..shards-1
    |> Enum.map(&({&1, Util.get_table_names(&1)}))
    |> Enum.into(%{})

    {:ok, %{tables: tables, shards: shards}}
  end

  @doc false
  @spec handle_call(:drop_cold_cache, any, map) :: {:reply, true, map}
  def handle_call(:drop_cold_cache, _from, %{shards: shards, tables: tables} = s) do
    Enum.map(0..shards-1, fn(shard) ->
      pool = Util.get_pool_name(shard)
      tables = Map.fetch!(tables, shard)
      :ok = lock_shard(pool)
      :ok = do_drop(tables)
      :ok = unlock_shard(pool)
    end)

    {:reply, true, s}
  end

  @spec lock_shard(atom) :: :ok
  defp lock_shard(pool) do
    SpaghettiPool.lock(pool)
  end

  @spec lock_shard(atom) :: :ok
  defp unlock_shard(pool) do
    SpaghettiPool.unlock(pool)
  end

  @spec do_drop({atom, atom, atom}) :: :ok
  defp do_drop({hot, cold, waiting}) do
    waiting = :ets.rename(cold, waiting) # Cold table is now the waiting table
    ^cold = :ets.rename(hot, cold) # Hot table is now the cold table
    pid = Process.whereis(GenerationalCache.TableManager)
    ^hot = :ets.new(hot, [:named_table, :public, :set, read_concurrency: true]) # Hot table is newly created
    true = :ets.give_away(hot, pid, [])
    Task.start(fn -> :ets.delete(waiting) end) # Delete old cold cache asynchronously.
    :ok
  end
end