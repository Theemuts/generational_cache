defmodule GenerationalCache.Task.SizeMonitorTest do
  use ExUnit.Case, async: false
  doctest GenerationalCache.Task.SizeMonitor

  alias GenerationalCache.Task.SizeMonitor
  alias GenerationalCache.Util

  setup do
    alias GenerationalCache.Support.TestHelper
    TestHelper.do_setup
  end

  test "does nothing if maximum size is not exceeded" do
    GenerationalCache.insert(1, 2)
    SizeMonitor.do_run
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) != []
  end

  test "drops cold cache if maximum size is exceeded" do
    Application.put_env(:generational_cache, :max_size, {100, :kb})
    Enum.map(1..500, &GenerationalCache.insert(&1, %{a: 1, b: [1,2,3,4]}))
    assert Util.calculate_size(:kb) > 100
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) != []

    SizeMonitor.do_run

    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == []
    assert :ets.lookup(GenerationalCache.Shard1.Cold, 1) != []

    SizeMonitor.do_run

    assert :ets.lookup(GenerationalCache.Shard1.Cold, 1) == []

    Application.delete_env(:generational_cache, :max_size)
  end
end
