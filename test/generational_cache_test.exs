defmodule GenerationalCacheTest do
  use ExUnit.Case, async: false
  doctest GenerationalCache

  setup do
    alias GenerationalCache.Support.TestHelper
    TestHelper.do_setup
  end

  test "values can be inserted synchronously" do
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == []
    assert GenerationalCache.insert(1, 2, 3) == :ok
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == [{1, 2, 3, 0}]
  end

  test "values can be inserted asynchronously" do
    assert GenerationalCache.insert(1, 2, 3, true) == :ok
    :timer.sleep(5)
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == [{1, 2, 3, 0}]
  end

  test "inserted values can be retrieved" do
    GenerationalCache.insert(1, 2, 3)
    assert GenerationalCache.get(1) == {:ok, 1, 2, 3}
  end

  test "value retrieved from the cold cache is moved to the hot cache" do
    :ets.insert(GenerationalCache.Shard1.Hot, {1, 2, 3, 0})
    assert GenerationalCache.get(1) == {:ok, 1, 2, 3}
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == [{1, 2, 3, 0}]
    assert :ets.lookup(GenerationalCache.Shard1.Cold, 1) == []
  end

  test "returns :error when the key does not exist" do
    assert GenerationalCache.get(1) == :error
  end

  test "values can be deleted synchronously" do
    GenerationalCache.insert(1, 2, 3)
    assert GenerationalCache.delete(1) == :ok
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == []
  end

  test "values can be deleted asynchronously" do
    GenerationalCache.insert(1, 2, 3)
    assert GenerationalCache.delete(1, true) == :ok
    :timer.sleep(5)
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == []
  end

  test "value is updated if a newer or equal version is provided" do
    GenerationalCache.insert(1, 2, 3)
    GenerationalCache.insert(1, 2, 4)
    assert GenerationalCache.get(1) == {:ok, 1, 2, 4}
    GenerationalCache.insert(1, 3, 4)
    assert GenerationalCache.get(1) == {:ok, 1, 3, 4}

    GenerationalCache.insert(2, %{updated_at: 1}, 3)
    GenerationalCache.insert(2, %{updated_at: 2}, 3)
    assert GenerationalCache.get(2) == {:ok, 2, %{updated_at: 2}, 3}
  end

  test "value is not updated if an older version is provided" do
    GenerationalCache.insert(1, 2, 3)
    GenerationalCache.insert(1, 2, 2)
    assert GenerationalCache.get(1) == {:ok, 1, 2, 3}

    GenerationalCache.insert(2, %{updated_at: 2}, 3)
    GenerationalCache.insert(2, %{updated_at: 1}, 3)
    assert GenerationalCache.get(2) == {:ok, 2, %{updated_at: 2}, 3}
  end

  test "unversioned values is always updated, unless it has an updated_at-field." do
    GenerationalCache.insert(1, 2)
    GenerationalCache.insert(1, 3)
    assert GenerationalCache.get(1) == {:ok, 1, 3, -1}

    GenerationalCache.insert(2, %{updated_at: 2})
    GenerationalCache.insert(2, %{updated_at: 3})
    assert GenerationalCache.get(2) == {:ok, 2, %{updated_at: 3}, -1}
    GenerationalCache.insert(2, %{updated_at: 2})
    assert GenerationalCache.get(2) == {:ok, 2, %{updated_at: 3}, -1}
  end
end
