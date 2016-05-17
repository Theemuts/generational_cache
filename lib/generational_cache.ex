defmodule GenerationalCache do

  alias GenerationalCache.Shard.Pool
  alias GenerationalCache.Shard.Pool.Worker
  alias GenerationalCache.Util

  @type key :: atom | binary
  @type data :: any
  @type version :: integer
  @type version_handler :: Module.t
  @type version_or_handler :: version | version_handler
  @type result :: :error | {:ok, {key, data, version}}

  @doc """
  Retrieve a key from the cache. It gets a worker from the worker pool of the
  shard that can contain this key. If the key is found, it is returned as a
  tuple. The first element is `:ok`. the second another tuple containing the
  key, data, and version. If it is not found, `:error` is returned.

  The key can be either an integer or a binary.
  """
  @spec get(key) :: result
  def get(key) do
    fun = &Worker.get(&1, key)
    perform(fun, :read, key)
  end

  @doc """
  Insert a key into the cache.

  This function expects four arguments:
    - `key`: The key this entry will have, must be an integer or a string.
    - `data`: The data associated with this key, can be anything.
    - `version`: The version of the data. The default value is -1, which
    ignores versioning of data. You can also use a module which implements
    the `GenerationalCache.VersionHandler` behaviour to implement your own
    version handling.
     - `async`: If set to true, the insert is handled asynchronously. False by default.
  """
  @spec insert(key, data, version_or_handler, boolean) :: :ok
  def insert(key, data, version_or_handler \\ -1, async \\ false) do
    fun = &Worker.insert(&1, key, data, version_or_handler, async)
    perform(fun, {:write, key}, key)
  end

  @doc """

  """
  @spec delete(key, boolean) :: :ok
  def delete(key, async \\ false) do
    fun = &Worker.delete(&1, key, async)
    perform(fun, {:write, key}, key)
  end

  @spec perform(fun, :read | {:write, key}, key) :: result | :ok
  defp perform(fun, type, key) do
    key
    |> Util.store_in
    |> Pool.transaction(type, fun)
  end
end