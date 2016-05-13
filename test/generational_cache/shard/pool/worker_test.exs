defmodule GenerationalCache.Shard.Pool.WorkerTest do
  use ExUnit.Case, async: false
  doctest GenerationalCache.Shard.Pool.Worker

  alias GenerationalCache.Shard.Pool.Worker, as: Worker

  setup do
    alias GenerationalCache.Support.TestHelper
    TestHelper.do_setup
  end

  @tables {GenerationalCache.Shard0.Hot, GenerationalCache.Shard0.Cold, GenerationalCache.Shard0.Dropped}

  test "a new worker can be spawned" do
    result = Worker.start_link(@tables)
    assert is_tuple(result)
    assert elem(result, 0) == :ok
  end

  test "worker can insert value synchronously" do
    {:ok, worker} = Worker.start_link(@tables)
    assert :ets.lookup(GenerationalCache.Shard0.Hot, 0) == []
    assert Worker.insert(worker, 0, 2, 3, false) == :ok
    assert :ets.lookup(GenerationalCache.Shard0.Hot, 0) == [{0, 2, 3, 0}]
  end

  test "worker can insert value asynchronously" do
    {:ok, worker} = Worker.start_link(@tables)
    assert Worker.insert(worker, 0, 2, 3, true) == :ok
    :timer.sleep(5)
    assert :ets.lookup(GenerationalCache.Shard0.Hot, 0) == [{0, 2, 3, 0}]
  end

  test "inserted values can be retrieved" do
    {:ok, worker} = Worker.start_link(@tables)
    Worker.insert(worker, 0, 2, 3, false)
    assert Worker.get(worker, 0) == {:ok, 0, 2, 3}
  end

  test "value retrieved from the cold cache is moved to the hot cache" do
    {:ok, worker} = Worker.start_link(@tables)
    :ets.insert(GenerationalCache.Shard0.Hot, {0, 2, 3, 0})
    assert Worker.get(worker, 0) == {:ok, 0, 2, 3}
    assert :ets.lookup(GenerationalCache.Shard0.Hot, 0) == [{0, 2, 3, 0}]
    assert :ets.lookup(GenerationalCache.Shard0.Cold, 0) == []
  end

  test "returns :error when the key does not exist" do
    {:ok, worker} = Worker.start_link(@tables)
    assert Worker.get(worker, 0) == :error
  end

  test "values can be deleted synchronously" do
    {:ok, worker} = Worker.start_link(@tables)
    Worker.insert(worker, 0, 2, 3, false)
    assert Worker.delete(worker, 0, false) == :ok
    assert :ets.lookup(GenerationalCache.Shard0.Hot, 0) == []
  end

  test "values can be deleted asynchronously" do
    {:ok, worker} = Worker.start_link(@tables)
    Worker.insert(worker, 0, 2, 3, false)
    assert Worker.delete(worker, 0, true) == :ok
    :timer.sleep(5)
    assert :ets.lookup(GenerationalCache.Shard1.Hot, 1) == []
  end

  test "value is updated if a newer or equal version is provided" do
    {:ok, worker} = Worker.start_link(@tables)
    Worker.insert(worker, 0, 2, 3, false)
    Worker.insert(worker, 0, 2, 4, false)
    assert Worker.get(worker, 0) == {:ok, 0, 2, 4}
    Worker.insert(worker, 0, 3, 4, false)
    assert Worker.get(worker, 0) == {:ok, 0, 3, 4}

    Worker.insert(worker, 2, %{updated_at: 1}, 3, false)
    Worker.insert(worker, 2, %{updated_at: 2}, 3, false)
    assert Worker.get(worker, 2) == {:ok, 2, %{updated_at: 2}, 3}
  end

  test "value is not updated if an older version is provided" do
    {:ok, worker} = Worker.start_link(@tables)
    Worker.insert(worker, 0, 2, 3, false)
    Worker.insert(worker, 0, 2, 2, false)
    assert Worker.get(worker, 0) == {:ok, 0, 2, 3}

    Worker.insert(worker, 2, %{updated_at: 2}, 3, false)
    Worker.insert(worker, 2, %{updated_at: 1}, 3, false)
    assert Worker.get(worker, 2) == {:ok, 2, %{updated_at: 2}, 3}
  end

  test "unversioned values is always updated, unless it has an updated_at-field." do
    {:ok, worker} = Worker.start_link(@tables)
    Worker.insert(worker, 0, 2, -1, false)
    Worker.insert(worker, 0, 2, -1, false)
    assert Worker.get(worker, 0) == {:ok, 0, 2, -1}

    Worker.insert(worker, 2, %{updated_at: 2}, -1, false)
    Worker.insert(worker, 2, %{updated_at: 3}, -1, false)
    assert Worker.get(worker, 2) == {:ok, 2, %{updated_at: 3}, -1}
    Worker.insert(worker, 2, %{updated_at: 2}, -1, false)
    assert Worker.get(worker, 2) == {:ok, 2, %{updated_at: 3}, -1}
  end
end
