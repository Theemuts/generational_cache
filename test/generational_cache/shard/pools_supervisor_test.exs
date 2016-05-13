defmodule GenerationalCache.Shard.PoolsSupervisorTest do
  use ExUnit.Case, async: false
  doctest GenerationalCache.Shard.PoolsSupervisor

  alias GenerationalCache.Shard.PoolsSupervisor

  test "is restarted after crash" do
    pid = Process.whereis(PoolsSupervisor)
    assert pid
    Process.exit(pid, :test_exit)
    :timer.sleep(100)
    assert Process.whereis(PoolsSupervisor)
    :timer.sleep(100)
  end
end
