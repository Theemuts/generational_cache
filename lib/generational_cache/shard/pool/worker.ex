defmodule GenerationalCache.Shard.Pool.Worker do
  @moduledoc false

  @retry_count 5

  use GenServer

  def start_link({_, _, _} = tables) do
    GenServer.start_link(__MODULE__, tables)
  end

  @doc """
  Find a key in the shard. First, we'll try to find the key in the hot cache,
  if we don't find it, we'll look for it in the cold cache. If a result is
  found in the cold cache, it is inserted into the hot cache. If a result is
  found in either cache, {:ok, id, data, version} is returned. If no value is
  found, :error is returned.
  """
  def get(worker, id) do
    GenServer.call(worker, {:get, id})
  end

  @doc """
  Insert a value into the cache. Four parameters are required, they are:
    - worker: the worker pid
    - id: the data id
    - data: the actual data
    - version: the current version of the data, required to prevent
      overwriting newer data. If set to -1, it is ignored.

  A fifth parameter, async, is optional. Be default it is false, and your
  insert does not return until the data is inserted. If it is set to true,
  it returns immediately.
  """
  def insert(worker, id, data, version, false) do
    GenServer.call(worker, {:insert, {id, data, version}})
  end

  def insert(worker, id, data, version, true) do
    GenServer.cast(worker, {:insert, {id, data, version}})
  end

  @doc """
  Delete a key from the shard. Removes the key and its associated data from
  both the hot and cold shard, and returns :ok.

  It acceots two arguments, worker and id, and an optional third argument to
  set asynchronicity.
  """
  def delete(worker, id, false) do
    GenServer.call(worker, {:delete, id})
  end

  def delete(worker, id, true) do
    GenServer.cast(worker, {:delete, id})
  end

  def init({a, b, c}) do
    {:ok, %{hot: a, cold: b, waiting: c}}
  end

  def handle_call({:get, id}, _from, %{hot: h, cold: c} = s) do
    data = case :ets.lookup(h, id) do
      [{^id, data, version, _}] -> {:ok, id, data, version}
      [] -> handle_cold_lookup(c, h, id)
    end

    {:reply, data, s}
  end

  def handle_call({:insert, {id, data, version}}, _from, %{hot: h} = s) do
    do_handle_insert(id, data, version, h)
    {:reply, :ok, s}
  end

  def handle_call({:delete, id}, _from, %{hot: h, cold: c} = s) do
    handle_delete(h, c, id)
    {:reply, :ok, s}
  end

  def handle_cast({:insert, {id, data, version}}, %{hot: h} = s) do
    do_handle_insert(id, data, version, h)
    {:noreply, s}
  end

  def handle_cast({:delete, id}, %{hot: h, cold: c} = s) do
    handle_delete(h, c, id)
    {:noreply, s}
  end

  defp handle_cold_lookup(cold, hot, id) do
    cold
    |> :ets.lookup(id)
    |> move_to_hot(cold, hot)
  end

  defp move_to_hot([{id, data, version, _}], cold, hot) do
    if do_handle_insert(id, data, version, hot) do
      :ets.delete(cold, id)
      {:ok, id, data, version}
    else
      :error
    end
  end

  defp move_to_hot([], _, _), do: :error

  defp do_handle_insert(id, data, version, hot, retry_count \\ 0)

  defp do_handle_insert(id, data, version, hot, retry_count) when retry_count < @retry_count do
    case acquire_lock(hot, id, data, version) do
      1 when version == -1 and is_map(data) ->
        [{^id, current_data, _, _}] = :ets.lookup(hot, id)
        updated_at = Map.get(data, :updated_at)
        current_updated_at = Map.get(current_data, :updated_at)
        handle_unversioned_insert(hot, id, data, current_data, updated_at, current_updated_at)
      1 when version == -1 ->
        handle_unversioned_insert(hot, id, data, nil, nil, nil)
      1 ->
        [{^id, current_data, current_version, _}] = :ets.lookup(hot, id)
        handle_insert(hot, id, data, current_data, version, current_version)
      _ ->
        wait_for = (2.2 |> :math.pow(retry_count) |> round)*6
        :timer.sleep(wait_for)
        do_wait(retry_count)
    end
  end

  defp do_handle_insert(_, _, _, _, _), do: false

  defp acquire_lock(hot, id, data \\ %{}, version \\ -1) do
    :ets.update_counter(hot, id, {4, 1}, {id, data, version, 0})
  end

  defp handle_unversioned_insert(h, id, data, _, updated_at, current_updated_at)
      when updated_at > current_updated_at
      when is_nil updated_at do
    :ets.insert(h, {id, data, -1, 0})
  end

  defp handle_unversioned_insert(h, id, _, current_data, _, _) do
    :ets.insert(h, {id, current_data, -1, 0})
  end

  # If updated_at-fields exist, use them. Insert newer values, with last write
  # wins-strategy for equal update times.
  defp handle_insert(h, id, data = %{updated_at: u1}, %{updated_at: u2}, version, _)
      when u1 >= u2 do
    :ets.insert(h, {id, data, version, 0})
  end

  # Ignore older versions.
  defp handle_insert(_, _, %{updated_at: _}, %{updated_at: _}, _, _) do
    true
  end

  # No update-field exists, so depend on the version. Same strategy for equal versions.
  defp handle_insert(h, id, data, _, version, current_version)
      when current_version <= version do
    :ets.insert(h, {id, data, version, 0})
  end

  # Ignore older versions.
  defp handle_insert(_, _, _, _, _, _) do
    true
  end

  defp do_wait(retry_count) do
    (2.2 |> :math.pow(retry_count) |> round)*6 |> :timer.sleep
  end

  defp handle_delete(hot, cold, id, retry_count \\ 0)

  defp handle_delete(hot, cold, id, retry_count) when retry_count < @retry_count do
    case acquire_lock(hot, id) do
      1 ->
        :ets.delete(hot, id)
        :ets.delete(cold, id)
      _ ->
        do_wait(retry_count)
        handle_delete(hot, cold, id, retry_count + 1)
    end
  end
end