defmodule GenerationalCacheTest do
  use ExUnit.Case, async: false
  doctest GenerationalCache

  alias GenerationalCache.Version.Lock

  setup do
    alias GenerationalCache.Support.TestHelper
    TestHelper.do_setup
  end

  test "values can be inserted synchronously" do
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == []
    assert GenerationalCache.insert(1, 2, 3) == :ok
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == [{1, 2, 3}]
  end

  test "values can be inserted asynchronously" do
    assert GenerationalCache.insert(1, 2, 3, true) == :ok
    :timer.sleep(50)
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == [{1, 2, 3}]
  end

  test "values can be inserted without version" do
    assert GenerationalCache.insert(1, 2) == :ok
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == [{1, 2, -1}]
  end

  test "inserted values can be retrieved" do
    GenerationalCache.insert(1, 2, 3)
    assert GenerationalCache.get(1) == {:ok, {1, 2, 3}}
  end

  test "value retrieved from the cold cache is moved to the hot cache" do
    :ets.insert(GenerationalCache.Shard1.Cold, {1, 2, 3})
    assert GenerationalCache.get(1) == {:ok, {1, 2, 3}}
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == [{1, 2, 3}]
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
    :timer.sleep(50)
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == []
  end

  test "value is updated if a newer or equal version is provided" do
    GenerationalCache.insert(1, 2, 3)
    GenerationalCache.insert(1, 2, 4)
    assert GenerationalCache.get(1) == {:ok, {1, 2, 4}}
    GenerationalCache.insert(1, 3, 4)
    assert GenerationalCache.get(1) == {:ok, {1, 3, 4}}

    GenerationalCache.insert(2, %{updated_at: 1}, 3)
    GenerationalCache.insert(2, %{updated_at: 2}, 3)
    assert GenerationalCache.get(2) == {:ok, {2, %{updated_at: 2}, 3}}
  end

  test "value is not updated if an older version is provided" do
    GenerationalCache.insert(1, 2, 3)
    GenerationalCache.insert(1, 2, 2)
    assert GenerationalCache.get(1) == {:ok, {1, 2, 3}}
  end

  test "version can be set with the value of an explicit lock field" do
    GenerationalCache.insert(1, %{val: 1, lock: 2}, {Lock, :lock})
    GenerationalCache.insert(1, %{val: 2, lock: 3}, {Lock, :lock})
    assert GenerationalCache.get(1) == {:ok, {1, %{val: 2, lock: 3}, 3}}
    GenerationalCache.insert(1, %{val: 3, lock: 2}, {Lock, :lock})
    assert GenerationalCache.get(1) == {:ok, {1, %{val: 2, lock: 3}, 3}}
  end
end
