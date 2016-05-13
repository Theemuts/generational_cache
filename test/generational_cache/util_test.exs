defmodule GenerationalCache.UtilTest do
  use ExUnit.Case, async: false
  doctest GenerationalCache.Util

  alias GenerationalCache.Util

  test "returns the pool a key is stored in" do
    assert Util.store_in(0) == GenerationalCache.Shard0.Pool
    assert Util.store_in(1) == GenerationalCache.Shard1.Pool
    assert Util.store_in("A") == GenerationalCache.Shard1.Pool
    assert Util.store_in("B") == GenerationalCache.Shard0.Pool
  end

  test "returns the shard's pool name" do
    assert Util.get_pool_name(0) == GenerationalCache.Shard0.Pool
  end

  test "returns the shard's tables" do
    assert Util.get_table_names(0) == {GenerationalCache.Shard0.Hot, GenerationalCache.Shard0.Cold, GenerationalCache.Shard0.Dropped}
  end

  test "returns total cache size" do
    assert Util.calculate_size(:kb) > 0
    assert Util.calculate_size(:mb) > 0
    assert Util.calculate_size(:gb) > 0
  end
end
