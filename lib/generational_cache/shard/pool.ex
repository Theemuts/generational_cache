defmodule GenerationalCache.Shard.Pool do
  @moduledoc false

  alias GenerationalCache.Shard.Pool.Worker

  @doc false
  def start_link(shard) do
    {:ok, _} = Application.ensure_all_started(:poolboy)
    tables = GenerationalCache.Util.get_table_names(shard)
    pool_name = GenerationalCache.Util.get_pool_name(shard)
    sup_name = Module.concat([GenerationalCache, "Shard#{shard}", Pool, Supervisor])

    opts = [worker_module: Worker,
            name: {:local, pool_name},
            size: Application.get_env(:generational_cache, :shard_pool_size, 10),
            max_overflow: Application.get_env(:generational_cache, :shard_max_overflow, 10)]

    children = [:poolboy.child_spec(pool_name, opts, tables)]
    sup_opts = [strategy: :one_for_one, name: sup_name]
    Supervisor.start_link(children, sup_opts)
  end

  @doc false
  def transaction(pool, fun, max_retries \\ 5)
  def transaction(pool, fun, max_retries) when max_retries > 0 do
    case :ets.lookup(GenerationalCache.Locks, pool) do
      [{^pool, 0}] -> :poolboy.transaction(pool, fun)
      [{^pool, 1}] ->
        :timer.sleep(10)
        transaction(pool, fun, max_retries - 1)
    end
  end

  def transaction(_, _, 0) do
    :locked
  end
end