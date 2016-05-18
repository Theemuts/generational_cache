defmodule GenerationalCache.Shard.Pool.Worker do
  @moduledoc """
  The worker module. Each cache shard has a pool of workers assigned to it,
  managed by `SpaghettiPool`. This module is for internal use only.
  """

  @type state :: map
  @type get :: {:get, GenerationalCache.key}
  @type insert :: {:insert, {GenerationalCache.key, GenerationalCache.data, GenerationalCache.version_or_handler}}
  @type delete :: {:delete, GenerationalCache.key}
  @type call_reply :: {:reply, GenerationalCache.result | :ok, state}

  @retry_count 5

  use GenServer

  alias GenerationalCache.Version.Unversioned

  @doc false
  @spec start_link({atom, atom, atom}) :: GenServer.on_start
  def start_link({_, _, _} = tables) do
    GenServer.start_link(__MODULE__, tables)
  end

  @doc """
  Look for a key in the shard. First, we'll try to find the key in the shard's
  hot cache, if we don't find it, we'll look for it in the cold cache. If a
  result is found in the cold cache, it is inserted into the hot cache and
  deleted from the cold cache.

  Two parameters are required, they are:
    - `worker`: the worker pid.
    - `key`: the data key.

  If a result is found in either cache, `{:ok, {key, data, version}}` is
  returned. If no value is found, `:error` is returned.
  """
  @spec get(pid, GenerationalCache.key) :: GenerationalCache.result
  def get(worker, key) do
    GenServer.call(worker, {:get, key})
  end

  @doc """
  Insert a value into the cache.

  Five parameters are required, they are:
    - `worker`: the worker pid.
    - `key`: the data key.
    - `data`: the actual data.
    - `version`: the version of the data, or the version handler.
    - `async`: if `true`, the function returns immediately and the insert is
    handled asynchronously. If set to `false`, this function blocks until the
    insert is completed.

    This function always returns `:ok`.
  """
  @spec insert(pid, GenerationalCache.key, GenerationalCache.data, GenerationalCache.version_or_handler, boolean) :: :ok
  def insert(worker, key, data, version, false) do
    GenServer.call(worker, {:insert, {key, data, version}})
  end

  def insert(worker, key, data, version, true) do
    GenServer.cast(worker, {:insert, {key, data, version}})
  end

  @doc """
  Delete a key from the shard. Removes the key and its associated data from
  both the hot and cold cache.

  Three parameters are required, they are:
    - `worker`: the worker pid.
    - `key`: the data key.
    - `async`: if `true`, the function returns immediately and the delete is
    handled asynchronously. If set to `false`, this function blocks until the
    delete is completed.

  This function always returns `:ok`.
  """
  @spec delete(pid, GenerationalCache.key, boolean) :: :ok
  def delete(worker, key, false) do
    GenServer.call(worker, {:delete, key})
  end

  def delete(worker, key, true) do
    GenServer.cast(worker, {:delete, key})
  end

  @doc false
  @spec init({atom, atom, atom}) :: {:ok, state}
  def init({hot, cold, dropped}) do
    {:ok, %{hot: hot, cold: cold, dropped: dropped}}
  end

  @spec handle_call(get | insert | delete, pid, state) :: call_reply
  def handle_call({:get, key}, _from, %{hot: h, cold: c} = s) do
    data = do_get(key, h, c)
    {:reply, data, s}
  end

  def handle_call({:insert, {key, data, version_handler}}, _from, %{hot: h, cold: c} = s)
      when is_atom(version_handler)
      or is_tuple(version_handler) do
    handle_insert(key, data, version_handler, h, c)
    {:reply, :ok, s}
  end

  def handle_call({:insert, {key, data, version}}, _from, %{hot: h, cold: c} = s)
      when is_integer(version) do
    handle_insert(key, data, version, h, c)
    {:reply, :ok, s}
  end

  def handle_call({:delete, key}, _from, %{hot: h, cold: c} = s) do
    handle_delete(key, h, c)
    {:reply, :ok, s}
  end

  @doc false
  @spec handle_cast(insert | delete, map) :: :ok
  def handle_cast({:insert, {key, data, version_handler}}, %{hot: h, cold: c} = s)
      when is_atom version_handler do
    handle_insert(key, data, version_handler, h, c)
    {:noreply, s}
  end

  def handle_cast({:insert, {key, data, version}}, %{hot: h, cold: c} = s)
      when is_integer(version) do
    handle_insert(key, data, version, h, c)
    {:noreply, s}
  end

  def handle_cast({:delete, key}, %{hot: h, cold: c} = s) do
    handle_delete(key, h, c)
    {:noreply, s}
  end

  @spec handle_cold_lookup(GenerationalCache.key, atom, atom) :: GenerationalCache.result
  defp handle_cold_lookup(key, cold, hot) do
    cold
    |> :ets.lookup(key)
    |> move_to_hot(cold, hot)
  end

  @spec move_to_hot({GenerationalCache.key, GenerationalCache.data, GenerationalCache.version}, atom, atom) :: GenerationalCache.result
  defp move_to_hot([{key, data, version}], cold, hot) do
    do_insert(key, data, version, hot, cold)
    :ets.delete(cold, key)
    {:ok, {key, data, version}}
  end

  defp move_to_hot([], _, _), do: :error

  @spec do_get(GenerationalCache.key, atom, atom) :: GenerationalCache.result
  defp do_get(key, hot, cold) do
    case :ets.lookup(hot, key) do
      [result] -> {:ok, result}
      [] -> handle_cold_lookup(key, cold, hot)
    end
  end

  @spec handle_insert(GenerationalCache.key, GenerationalCache.data, GenerationalCache.version_or_version_handler, atom, atom) :: boolean
  defp handle_insert(key, data, Unversioned, h, c) do
    {:ok, {^data, -1}} = Unversioned.handle_insert(data)
    do_insert(key, data, -1, h, c)
  end

  defp handle_insert(key, data, version_handler, h, c) when is_atom(version_handler) or is_tuple(version_handler) do
    {old_data, old_version} = case do_get(key, h, c) do
      {:ok, {^key, old_data, old_version}} -> {old_data, old_version}
      _ -> {nil, nil}
    end

    version_handler
    |> check_version(data, old_data, old_version)
    |> maybe_insert(key, h, c)
  end

  defp handle_insert(key, data, version, h, c) when is_integer version do
    case do_get(key, h, c) do
      {:ok, {^key, _old_data, old_version}} ->
        if version >= old_version, do: do_insert(key, data, version, h, c)
      _ ->
        do_insert(key, data, version, h, c)
    end
  end

  @spec do_insert(GenerationalCache.key, GenerationalCache.data, GenerationalCache.version, atom, atom) :: true
  defp do_insert(key, data, version, hot, cold) do
    :ets.insert(hot, {key, data, version})
    :ets.delete(cold, key)
  end

  @spec handle_delete(GenerationalCache.key, atom, atom) :: true
  defp handle_delete(key, hot, cold) do
    :ets.delete(hot, key)
    :ets.delete(cold, key)
  end

  @spec check_version({Module.t, any} | Module.t, GenerationalCache.data, GenerationalCache.data, GenerationalCache.version) :: :error | {:ok, {GenerationalCache.data, GenerationalCache.version}}
  defp check_version({handler, opts}, data, old_data, old_version) do
    handler.handle_insert(data, old_data, old_version, opts)
  end

  defp check_version(handler, data, old_data, old_version) do
    handler.handle_insert(data, old_data, old_version, [])
  end

  @spec maybe_insert(:error | {:ok, {GenerationalCache.data, GenerationalCache.version}}, GenerationalCache.key, atom, atom) :: boolean
  defp maybe_insert(:error, _, _, _), do: false
  defp maybe_insert({:ok, {data, version}}, key, h, c), do: do_insert(key, data, version, h, c)
end