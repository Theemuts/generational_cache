defmodule GenerationalCache.Task.SizeMonitor do
  @moduledoc """
  This background task is responsible for monitoring the size of the cache,
  and will call `GenerationalCache.CacheDropServer` to drop the cold cache
  if he maximum size is exceeded.

  For configuration options, see the `GenerationalCache` module documentation.
  """

  alias GenerationalCache.Util

  @doc false
  def run do
    :generational_cache
    |> Application.get_env(:size_monitor_interval, 60_000)
    |> :timer.sleep

    do_run
    run
  end

  @doc false
  @spec do_run :: boolean
  def do_run do
    {size_limit, unit} = Application.get_env(:generational_cache, :max_size, {1, :gb})
    cache_size = Util.calculate_size(unit)
    cache_size > size_limit and GenerationalCache.CacheDropServer.drop_cold_cache
  end
end