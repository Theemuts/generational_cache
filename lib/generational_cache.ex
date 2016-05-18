defmodule GenerationalCache do
  @moduledoc """
  This application provides an ETS-table based cache.
  """

  alias GenerationalCache.Shard.Pool
  alias GenerationalCache.Shard.Pool.Worker
  alias GenerationalCache.Util
  alias GenerationalCache.Version.Unversioned

  @timeout 5_000

  @type key :: atom | binary
  @type data :: any
  @type version :: integer
  @type version_handler :: Module.t | {Module.t, any}
  @type version_or_handler :: version | version_handler
  @type result :: :error | {:ok, {key, data, version}}

  @doc """
  Retrieve a key from the cache. It gets a worker from the worker pool of the
  shard that can contain this key. If the key is found, it is returned as a
  tuple. The first element is `:ok`. the second another tuple containing the
  key, data, and version. If it is not found, `:error` is returned.

  The key can be either an integer or a binary.
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
    - `version`: The version of the data. The default value is -1, which
    ignores versioning of data. You can also use a module which implements
    the `GenerationalCache.VersionHandler` behaviour to implement your own
    version handling.
     - `async`: If set to true, the insert is handled asynchronously. False by
     default.
     - `timeout`: The maximum time in milliseconds spent waiting for a worker
     before timing out. 5000 milliseconds by default.
  """
  @spec insert(key, data, version_or_handler, boolean, integer) :: :ok
  def insert(key, data, version_or_handler \\ Unversioned, async \\ false, timeout \\ @timeout) do
    fun = &Worker.insert(&1, key, data, version_or_handler, async)
    perform(fun, {:write, key}, key, timeout)
  end

  @doc """
  Delete a key from the cache.
  """
  @spec delete(key, boolean, integer) :: :ok
  def delete(key, async \\ false, timeout \\ @timeout) do
    fun = &Worker.delete(&1, key, async)
    perform(fun, {:write, key}, key, timeout)
  end

  @spec perform(fun, :read | {:write, key}, key, integer) :: result | :ok
  defp perform(fun, type, key, timeout) do
    key
    |> Util.store_in
    |> Pool.transaction(type, fun, timeout)
  end
end