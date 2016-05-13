defmodule GenerationalCache.Shard.PoolTest do
  use ExUnit.Case, async: false
  doctest GenerationalCache.Shard.Pool

  alias GenerationalCache.Shard1.Pool.Supervisor, as: S

  test "is restarted after crash" do
    pid = Process.whereis(S)
    assert pid
    Process.exit(pid, :test_exit)
    :timer.sleep(100)
    assert Process.whereis(S)
    :timer.sleep(100)
  end
end
