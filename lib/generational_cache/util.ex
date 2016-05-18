defmodule GenerationalCache.Util do
  @moduledoc """
  Helper functions mostly intended for internal use.
  """

  @doc """
  Computes the shard data should be stored in. Accepts integer and strings.
  If an integer id is used, the data will be stored in `rem(id, shards)`,
  if a string id is used, the code points are summed and the remainder of that
  result divided by `shards` will be the shard the data is stored in.

  This method has been chosen because its results are consistent and works
  with both UUIDs and other string-based id-schemes.
  """
  # TODO: Enforcing power-of-2 shards might avoid rem. Measure this.
  @spec store_in(GenerationalCache.key) :: atom
  def store_in(id) when is_integer(id) do
    shards = Application.get_env(:generational_cache, :shards, 2)
    id
    |> rem(shards)
    |> get_pool_name
  end

  def store_in(id) when is_binary(id) do
    shards = Application.get_env(:generational_cache, :shards, 2)

    id
    |> to_char_list
    |> Enum.reduce(&(&1 + &2))
    |> rem(shards)
    |> get_pool_name
  end

  @doc """
  Returns the name of the pool associated with the given shard.
  """
  @spec get_pool_name(integer) :: atom
  def get_pool_name(shard) do
    Module.concat([GenerationalCache, "Shard#{shard}", Pool])
  end

  @doc """
  Returns the tables associated with the given shard.
  """
  @spec get_table_names(integer) :: {atom, atom, atom}
  def get_table_names(shard) do
    hot = table_name(shard, :hot)
    cold = table_name(shard, :cold)
    dropped = table_name(shard, :dropped)

    {hot, cold, dropped}
  end

  @doc """
  Returns the current cache size in the given unit. Accepted units are:
    - `:kb`
    - `:mb`
    - `:gb`
  """
  @spec calculate_size(:kb | :mb | :gb) :: integer
  def calculate_size(unit) do
    shards = Application.get_env(:generational_cache, :shards, 2) - 1
    0..shards
    |> Enum.reduce(0, &((&1 |> get_table_names |> get_memory_size(unit)) + &2))
  end

  defp table_name(shard, :hot), do: Module.concat("GenerationalCache.Shard#{shard}", Hot)
  defp table_name(shard, :cold), do: Module.concat("GenerationalCache.Shard#{shard}", Cold)
  defp table_name(shard, :dropped), do: Module.concat("GenerationalCache.Shard#{shard}", Dropped)

  defp get_memory_size({hot, cold, _}, :kb) do
    get_memory_words(hot, cold) * :erlang.system_info(:wordsize) / 1024
  end

  defp get_memory_size({hot, cold, _}, :mb) do
    get_memory_words(hot, cold) * :erlang.system_info(:wordsize) / 1048576
  end

  defp get_memory_size({hot, cold, _}, :gb) do
    get_memory_words(hot, cold) * :erlang.system_info(:wordsize) / 1073741824
  end

  defp get_memory_words(hot, cold) do
    :ets.info(hot, :memory) + :ets.info(cold, :memory)
  end
end