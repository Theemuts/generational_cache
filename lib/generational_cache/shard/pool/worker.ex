defmodule GenerationalCache.Shard.Pool.Worker do
  @moduledoc """
  The Worker module. Each cache shard has a pool of workers assigned to it,
  managed by `poolboy`. This module is for internal use only.
  """

  @type state :: map
  @type get :: {:get, GenerationalCache.key}
  @type insert :: {:insert, {GenerationalCache.key, GenerationalCache.data, GenerationalCache.version}}
  @type delete :: {:delete, GenerationalCache.key}
  @type get_reply :: {:ok, {GenerationalCache.key, GenerationalCache.data, GenerationalCache.version}} | :error
  @type call_reply :: {:reply, get_reply | :ok, state}

  @retry_count 5

  use GenServer

  @doc false
  @spec start_link({atom, atom, atom}) :: GenServer.on_start
  def start_link({_, _, _} = tables) do
    GenServer.start_link(__MODULE__, tables)
  end

  @doc """
  Find a key in the shard. First, we'll try to find the key in the hot cache,
  if we don't find it, we'll look for it in the cold cache. If a result is
  found in the cold cache, it is inserted into the hot cache. If a result is
  found in either cache, {:ok, {key, data, version}} is returned. If no value is
  found, :error is returned.
  """
  @spec get(pid, GenerationalCache.key) :: GenerationalCache.result
  def get(worker, key) do
    GenServer.call(worker, {:get, key})
  end

  @doc """
  Insert a value into the cache. Four parameters are required, they are:
    - worker: the worker pid
    - key: the data key
    - data: the actual data
    - version: the current version of the data, required to prevent
      overwriting newer data. If set to -1, it is ignored.

  A fifth parameter, async, is optional. Be default it is false, and your
  insert does not return until the data is inserted. If it is set to true,
  it returns immediately.
  """
  @spec insert(pid, GenerationalCache.key, GenerationalCache.data, GenerationalCache.version | GenerationalCache.version_handler, boolean) :: :ok
  def insert(worker, key, data, version, false) do
    GenServer.call(worker, {:insert, {key, data, version}})
  end

  def insert(worker, key, data, version, true) do
    GenServer.cast(worker, {:insert, {key, data, version}})
  end

  @doc """
  Delete a key from the shard. Removes the key and its associated data from
  both the hot and cold shard, and returns :ok.

  It acceots two arguments, worker and key, and an optional third argument to
  set asynchronicity.
  """
  @spec delete(pid, GenerationalCache.key, boolean) :: :ok
  def delete(worker, key, false) do
    GenServer.call(worker, {:delete, key})
  end

  def delete(worker, key, true) do
    GenServer.cast(worker, {:delete, key})
  end

  @spec init({atom, atom, atom}) :: {:ok, state}
  def init({hot, cold, dropped}) do
    {:ok, %{hot: hot, cold: cold, dropped: dropped}}
  end

  @spec handle_call(get | insert | delete, pid, state) :: call_reply
  def handle_call({:get, key}, _from, %{hot: h, cold: c} = s) do
    data = do_get(h, c, key)
    {:reply, data, s}
  end

  def handle_call({:insert, {key, data, version}}, _from, %{hot: h, cold: c} = s) when is_atom version do
    initial_version = version.handle_insert(data, [])
    do_handle_insert(key, data, version, initial_version, h, c)
    {:reply, :ok, s}
  end

  def handle_call({:insert, {key, data, version}}, _from, %{hot: h, cold: c} = s) do
    do_handle_insert(key, data, version, version, h, c)
    {:reply, :ok, s}
  end

  def handle_call({:delete, key}, _from, %{hot: h, cold: c} = s) do
    handle_delete(h, c, key)
    {:reply, :ok, s}
  end

  @spec handle_cast(insert | delete, map) :: :ok
  def handle_cast({:insert, {key, data, version}}, %{hot: h, cold: c} = s) when is_atom version do
    initial_version = version.handle_insert(data, [])
    do_handle_insert(key, data, version, initial_version, h, c)
    {:noreply, s}
  end

  def handle_cast({:insert, {key, data, version}}, %{hot: h, cold: c} = s) do
    do_handle_insert(key, data, version, version, h, c)
    {:noreply, s}
  end

  def handle_cast({:delete, key}, %{hot: h, cold: c} = s) do
    handle_delete(h, c, key)
    {:noreply, s}
  end

  @spec handle_cold_lookup(atom, atom, GenerationalCache.key) ::  get_reply
  defp handle_cold_lookup(cold, hot, key) do
    cold
    |> :ets.lookup(key)
    |> move_to_hot(cold, hot)
  end

  @spec move_to_hot({GenerationalCache.key, GenerationalCache.data, GenerationalCache.version, integer}, atom, atom) ::  get_reply
  defp move_to_hot([{key, data, version, _}], cold, hot) do
    if do_handle_insert(key, data, version, version, hot, cold) do
      :ets.delete(cold, key)
      {:ok, {key, data, version}}
    else
      :error
    end
  end

  defp move_to_hot([], _, _), do: :error

  defp do_get(hot, cold, key) do
    case :ets.lookup(hot, key) do
      [{^key, data, version, _}] -> {:ok, {key, data, version}}
      [] -> handle_cold_lookup(cold, hot, key)
    end
  end

  @spec do_handle_insert(GenerationalCache.key, GenerationalCache.data, GenerationalCache.version_or_handler, GenerationalCache.version, atom, atom, integer) :: boolean
  defp do_handle_insert(key, data, version, initial_version, hot, cold, retry_count \\ 0)

  defp do_handle_insert(key, data, version, initial_version, hot, cold, retry_count) when retry_count < @retry_count do
    case acquire_lock_or_insert(hot, key, data, version) do
      0 -> true # Lock = 0 indicates no value existed yet.
      1 when version == -1 when is_atom(version) ->
        [{^key, current_data, _, _}] = :ets.lookup(hot, key)
        updated_at = Map.get(data, :updated_at)
        current_updated_at = Map.get(current_data, :updated_at)
        handle_unversioned_insert(hot, key, data, current_data, updated_at, current_updated_at)
      1 when version == -1 ->
        handle_unversioned_insert(hot, key, data, nil, nil, nil)
      1 ->
        [{^key, current_data, current_version, _}] = :ets.lookup(hot, key)
        handle_insert(hot, key, data, current_data, version, current_version)
      _ ->
        wait_for = (2.2 |> :math.pow(retry_count) |> round)*6
        :timer.sleep(wait_for)
        do_wait(retry_count)
        do_handle_insert(key, data, version, initial_version, hot, cold, retry_count - 1)
    end
  end

  defp do_handle_insert(_, _, _, _, _, _, _), do: false

  @spec acquire_lock_or_insert(atom, GenerationalCache.key, GenerationalCache.data, GenerationalCache.version) :: integer
  defp acquire_lock_or_insert(hot, key, data \\ %{}, version) when is_integer(version) do
    :ets.update_counter(hot, key, {4, 1}, {key, data, version, -1})
  end

  defp handle_unversioned_insert(h, key, data, _, updated_at, current_updated_at)
      when updated_at > current_updated_at
      when is_nil updated_at do
    :ets.insert(h, {key, data, -1, 0})
  end

  defp handle_unversioned_insert(h, key, _, current_data, _, _) do
    :ets.insert(h, {key, current_data, -1, 0})
  end

  # If updated_at-fields exist, use them. Insert newer values, with last write
  # wins-strategy for equal update times.
  defp handle_insert(h, key, data = %{updated_at: u1}, %{updated_at: u2}, version, _)
      when u1 >= u2 do
    :ets.insert(h, {key, data, version, 0})
  end

  # Ignore older versions.
  defp handle_insert(_, _, %{updated_at: _}, %{updated_at: _}, _, _) do
    true
  end

  # No update-field exists, so depend on the version. Same strategy for equal versions.
  defp handle_insert(h, key, data, _, version, current_version)
      when current_version <= version do
    :ets.insert(h, {key, data, version, 0})
  end

  # Ignore older versions.
  defp handle_insert(_, _, _, _, _, _) do
    true
  end

  defp do_wait(retry_count) do
    (2.2 |> :math.pow(retry_count) |> round)*6 |> :timer.sleep
  end

  defp handle_delete(hot, cold, key, retry_count \\ 0)

  defp handle_delete(hot, cold, key, retry_count) when retry_count < @retry_count do
    case acquire_lock_or_insert(hot, key, 999999) do
      1 ->
        :ets.delete(hot, key)
        :ets.delete(cold, key)
      _ ->
        do_wait(retry_count)
        handle_delete(hot, cold, key, retry_count + 1)
    end
  end
end