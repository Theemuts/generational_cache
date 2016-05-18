defmodule GenerationalCache.Shard.Pool do
  @moduledoc false

  alias GenerationalCache.Shard.Pool.Worker

  @doc false
  @spec start_link(integer) :: Supervisor.on_start
  def start_link(shard) do
    {:ok, _} = Application.ensure_all_started(:spaghetti_pool)
    tables = GenerationalCache.Util.get_table_names(shard)
    pool_name = GenerationalCache.Util.get_pool_name(shard)
    sup_name = Module.concat([GenerationalCache, "Shard#{shard}", Pool, Supervisor])

    IO.puts "Start pool #{shard}"
    opts = [worker_module: Worker,
            name: {:local, pool_name},
            size: Application.get_env(:generational_cache, :shard_pool_size, 10),
            max_overflow: Application.get_env(:generational_cache, :shard_max_overflow, 10)]

    children = [SpaghettiPool.child_spec(pool_name, opts, tables)]
    sup_opts = [strategy: :one_for_one, name: sup_name]
    Supervisor.start_link(children, sup_opts)
  end

  @doc false
  @spec transaction(Module.t, atom | {atom, any}, fun, integer) :: term
  def transaction(pool, type, fun, timeout)
  def transaction(pool, type, fun, timeout) do
    SpaghettiPool.transaction(pool, type, fun, timeout)
  end
end