defmodule GenerationalCache.CacheDropServerTest do
  use ExUnit.Case, async: false
  doctest GenerationalCache.CacheDropServer

  alias GenerationalCache.CacheDropServer

  setup do
    alias GenerationalCache.Support.TestHelper
    TestHelper.do_setup
  end

  test "initializes state" do
    assert CacheDropServer.init(:ok) == {:ok, %{shards: 2, tables: %{0 => {GenerationalCache.Shard0.Hot, GenerationalCache.Shard0.Cold, GenerationalCache.Shard0.Dropped}, 1 => {GenerationalCache.Shard1.Hot, GenerationalCache.Shard1.Cold, GenerationalCache.Shard1.Dropped}}}}
  end

  test "is restarted after crash" do
    pid = Process.whereis(CacheDropServer)
    assert pid
    Process.exit(pid, :test_exit)
    :timer.sleep(100)
    assert Process.whereis(CacheDropServer)
    :timer.sleep(100)
  end

  test "drops cold cache" do
    :ets.insert(GenerationalCache.Shard0.Hot, {0, 2, 3, 0})
    :ets.insert(GenerationalCache.Shard1.Hot, {1, 2, 3, 0})
    :ets.insert(GenerationalCache.Shard0.Cold, {2, 2, 3, 0})
    :ets.insert(GenerationalCache.Shard1.Cold, {3, 2, 3, 0})

    CacheDropServer.drop_cold_cache

    assert :ets.lookup(GenerationalCache.Shard0.Hot, 0) == []
    assert :ets.lookup(GenerationalCache.Shard0.Cold, 0) == [{0, 2, 3, 0}]
    assert :ets.lookup(GenerationalCache.Shard0.Cold, 2) == []
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == []
    assert :ets.lookup(GenerationalCache.Shard1.Cold, 1) == [{1, 2, 3, 0}]
    assert :ets.lookup(GenerationalCache.Shard1.Cold, 3) == []
  end
end
