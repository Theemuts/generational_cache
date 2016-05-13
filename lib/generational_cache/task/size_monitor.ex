defmodule GenerationalCache.Task.SizeMonitor do
  @moduledoc false

  alias GenerationalCache.Util

  def run do
    :generational_cache
    |> Application.get_env(:size_monitor_interval, 60_000)
    |> :timer.sleep

    do_run
    run
  end

  def do_run do
    {size_limit, unit} = Application.get_env(:generational_cache, :max_size, {1, :gb})
    cache_size = Util.calculate_size(unit)
    cache_size > size_limit && GenerationalCache.CacheDropServer.drop_cold_cache
  end
end