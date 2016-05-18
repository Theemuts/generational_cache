defmodule GenerationalCache.TableManagerTest do
  use ExUnit.Case, async: false
  doctest GenerationalCache.TableManager

  alias GenerationalCache.TableManager

  test "initializes all tables" do
    shards = Application.get_env(:generational_cache, :shards, 2)

    tables = 0..shards-1
    |> Enum.reduce([], fn(shard, acc) ->
         {a, b, c} = GenerationalCache.Util.get_table_names(shard)
         [a, b, c | acc]
       end)

    tables
    |> Enum.chunk(2, 3)
    |> Enum.map(&Enum.map(&1, fn(t) -> :ets.delete(t) end))

    init = TableManager.init(:ok)

    assert init == {:ok, tables}
  end

  test "is restarted after crash" do
    pid = Process.whereis(TableManager)
    assert pid
    ref = Process.monitor(pid)
    Process.exit(pid, :test_exit)
    assert_receive {:DOWN, ^ref, :process, ^pid, :test_exit}

    :timer.sleep(100)
    assert Process.whereis(TableManager)
  end
end
