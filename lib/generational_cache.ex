defmodule GenerationalCache do
  @moduledoc """
  This application provides an ETS-table based cache. Because of the overhead
  involved with maintaining an LRU cache, this cache works differently.

  The cache is divided in a number of shards, a key is consistenly stored in a
  single shard. Each shard has two levels, hot and cold. New data is inserted
  into the cold cache, values retrieved from the cold cache are inserted into
  the hot cache. When the maximum size is exceeded, the cold cache of every
  shard is deleted. The hot cache is turned into the cold cache, and a new hot
  cache is created.

  There are several settings you can adjust in your configuration file by
  setting `config :generational_cache, [k1: v1, ..., kn: vn]`. They are:

    - `:shards`, the number of shards. Defaults to 2.
    - `:shard_pool_size`, the number of workers a shard has. Defaults to 10.
    - `:shard_max_overflow`, the maximum number of additional workers a shard
    creates. Defaults to 10.
    - `:size_monitor_interval`, the time in milliseconds that the size monitor
    sleeps before checking the cache size again. Defaults to 60000.
    - `:max_size`, the maximum size of all ETS tables used by this
    application. Expects a tuple `{value, unit}`, where value is an integer
    and `unit` either `:kb`, `:mb`, or `:gb`. Defaults to `{1, :gb}`.

  It is recommended that you use some versioning system to avoid overwriting
  more recent entries with stale data. See `GenerationalCache.insert/5` and
  `GenerationalCache.Version` for more information.
  """

  alias GenerationalCache.Shard.Pool
  alias GenerationalCache.Shard.Pool.Worker
  alias GenerationalCache.Util

  @timeout 5_000

  @type key :: atom | binary
  @type data :: any
  @type version :: integer
  @type version_handler :: Module.t | {Module.t, any}
  @type version_or_handler :: version | version_handler
  @type result :: :error | {:ok, {key, data, version}}

  @doc """
  Retrieve a key from the cache. It gets a worker from the worker pool of the
  shard that can contain this key.

  This function expects two arguments:
    - `key`: the key you want to look up.
    - `timeout`: The maximum time in milliseconds spent waiting for a worker
    before timing out. 5000 milliseconds by default.

  If the key is found, it is returned as a tuple:
  `{:ok, {key, data, version}}`. If it is not found, `:error` is returned.
  """
  @spec get(key, integer) :: result
  def get(key, timeout \\ @timeout) do
    fun = &Worker.get(&1, key)
    perform(fun, :read, key, timeout)
  end

  @doc """
  Insert a key into the cache.

  This function expects five arguments:
    - `key`: The key this entry will have, must be an integer or a string.
    - `data`: The data associated with this key, can be anything.
    - `version`: The version of the data, or the version handling module.
    Defaults to `GenerationalCache.Version.Unversioned`, which always inserts
    the data and sets the version to -1.
     - `async`: If set to true, the insert is handled asynchronously. False by
     default.
     - `timeout`: The maximum time in milliseconds spent waiting for a worker
     before timing out. 5000 milliseconds by default.

  It is likely that under high load the default version handler will cause
  your cache to become inconsistent. You can implement your own version
  handling with the `GenerationalCache.Version` behaviour. If the data you are
  inserting is optimistically locked Ecto data, you can use
  `{GenerationalCache.Version.Lock, field}` (replace `field` with the field
  that holds the lock value). See `GenerationalCache.Version.Lock` for more
  information.

  This function always returns `:ok`.
  """
  @spec insert(key, data, version_or_handler, boolean, integer) :: :ok
  def insert(key, data, version_or_handler \\ GenerationalCache.Version.Unversioned, async \\ false, timeout \\ @timeout) do
    fun = &Worker.insert(&1, key, data, version_or_handler, async)
    perform(fun, {:write, key}, key, timeout)
  end

  @doc """
  Delete a key from the cache.

  This function expects three arguments:
    - `key`: The key this entry will have, must be an integer or a string.
     - `async`: If set to true, the insert is handled asynchronously. False by
     default.
     - `timeout`: The maximum time in milliseconds spent waiting for a worker
     before timing out. 5000 milliseconds by default.

  This function always returns `:ok`.
  """
  @spec delete(key, boolean, integer) :: :ok
  def delete(key, async \\ false, timeout \\ @timeout) do
    fun = &Worker.delete(&1, key, async)
    perform(fun, {:write, key}, key, timeout)
  end

  def get_avg(list) do
    Enum.reduce(list, &(&1+&2))/length(list)
  end

  def get_std(list, avg) do
    :math.sqrt(Enum.reduce(list, &(:math.pow((&1-avg),2)+&2))/length(list))
  end

  def repeatedly(n, ch_s, m) do
    y = for _ <- 1..m, do: test(n, ch_s)
    :timer.sleep(50)
    avg = get_avg(y)
    std = get_std(y, avg)
    {avg, std}
  end

  def test do
    for i <- 1..20 do
      {i*100, repeatedly(i*100, 30, 100)}
    end
  end

  def test(n, ch_s) do
    GenerationalCache.CacheDropServer.drop_cold_cache
    :timer.sleep(50)
    GenerationalCache.CacheDropServer.drop_cold_cache

    i = 1..n
    |> Enum.chunk(ch_s)

    y = %{a: 1, b: 2, c: 3}

    :timer.tc(fn ->
      Enum.map(i, fn(chnk) ->
        Task.async(fn ->
          Enum.map(chnk, fn(el) ->
            insert(el, y, GenerationalCache.Version.Unversioned, false, 1000000)
          end)
        end)
      end)
      |> Enum.map(&Task.await(&1, 10000000))
      :ok
    end)
    |> elem(0)
  end

  @spec perform(fun, :read | {:write, key}, key, integer) :: result | :ok
  defp perform(fun, type, key, timeout) do
    key
    |> Util.store_in
    |> Pool.transaction(type, fun, timeout)
  end
end