defmodule GenerationalCache.Version.Lock do
  @moduledoc """
  This version handler lets you use a lock-field of an inserted map as its
  version. This is useful, for example, in combination with optimistically
  locked Ecto data.

  To use this handler, pass `{GenerationalCache.Version.Lock, field}` as third
  parameter to `GenerationalCache.insert/5`. In this tuple, `field` must be
  the field that contains the version of the map.

  If you try to insert data which is not a map, or lacks the given lock-field,
  this function returns `:error`.
  """

  @behaviour GenerationalCache.Version

  @doc false
  def handle_insert(new_data, _, _, _) when not is_map(new_data), do: :error

  def handle_insert(new_data, nil, nil, field) do
    if Map.has_key?(new_data, field) do
      new_version = new_data[field]
      {:ok, {new_data, new_version}}
    else
      :error
    end
  end

  def handle_insert(new_data, _current_data, current_version, field) do
    new_version = Map.fetch!(new_data, field)
    if new_version >= current_version, do: {:ok, {new_data, new_version}}, else: :error
  end
end